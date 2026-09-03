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
	"hash"
	"math"
	"time"
	"unicode/utf8"

	"fortio.org/safecast"
)

const writeGroupDigestDomain = "open-splunk/write-group-membership/v1\x00"

var _ WriteGroupSequencer = (*SQLiteSequencer)(nil)

// WriteGroupMembershipSHA256 seals all values which determine physical replay.
// Members must be in strictly increasing visibility-sequence order.
func WriteGroupMembershipSHA256(members []Reservation) ([32]byte, error) {
	if len(members) == 0 || len(members) > MaxWriteGroupMembers {
		return [32]byte{}, fmt.Errorf("%w: write group member count is invalid", ErrInvalidArgument)
	}
	digest := sha256.New()
	_, _ = digest.Write([]byte(writeGroupDigestDomain))
	writeDigestUint64(digest, uint64(len(members)))
	var previous uint64
	for ordinal, member := range members {
		if member.Sequence == 0 || member.Sequence > math.MaxInt64 ||
			(ordinal > 0 && member.Sequence <= previous) ||
			member.BatchKey == "" || len(member.BatchKey) > maxBatchKeyBytes ||
			!utf8.ValidString(member.BatchKey) || len(member.Outbox) == 0 ||
			len(member.Outbox) > MaxOutboxBytes || member.StoredRowCount == 0 ||
			member.StoredRowCount > 1_000 || member.DecodedEventBytes == 0 ||
			member.DecodedEventBytes > 8<<20 {
			return [32]byte{}, fmt.Errorf("%w: write group member %d is invalid", ErrInvalidArgument, ordinal)
		}
		actualOutboxSHA256 := sha256.Sum256(member.Outbox)
		if actualOutboxSHA256 != member.OutboxSHA256 {
			return [32]byte{}, fmt.Errorf("write group member %d outbox digest mismatch", ordinal)
		}
		writeDigestUint64(digest, uint64(ordinal))
		writeDigestUint64(digest, member.Sequence)
		writeDigestBytes(digest, []byte(member.BatchKey))
		_, _ = digest.Write(member.PayloadSHA256[:])
		writeDigestUint64(digest, uint64(len(member.Outbox)))
		_, _ = digest.Write(member.OutboxSHA256[:])
		writeDigestUint64(digest, uint64(member.StoredRowCount))
		writeDigestUint64(digest, member.DecodedEventBytes)
		previous = member.Sequence
	}
	var result [32]byte
	copy(result[:], digest.Sum(nil))
	return result, nil
}

func writeDigestUint64(destination hash.Hash, value uint64) {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], value)
	_, _ = destination.Write(encoded[:])
}

func writeDigestBytes(destination hash.Hash, value []byte) {
	writeDigestUint64(destination, uint64(len(value)))
	_, _ = destination.Write(value)
}

// ValidateWriteGroup revalidates persisted membership, aggregate accounting,
// identity, and the sealed SHA-256 before a ClickHouse side effect is allowed.
func ValidateWriteGroup(group WriteGroup, limits WriteGroupLimits) error {
	if err := validateWriteGroupLimits(limits); err != nil {
		return err
	}
	if err := validateWriteGroupID(group.ID); err != nil {
		return err
	}
	if group.State != WriteGroupReady && group.State != WriteGroupAmbiguous {
		return fmt.Errorf("%w: write group is not sendable", ErrInvalidArgument)
	}
	memberCount, err := safecast.Conv[uint32](len(group.Members))
	if err != nil {
		return fmt.Errorf("%w: write group member count is invalid", ErrInvalidArgument)
	}
	if group.MemberCount == 0 || group.MemberCount != memberCount ||
		group.MemberCount > limits.MaxMembers || group.RowCount == 0 ||
		group.RowCount > limits.MaxRows || group.DecodedBytes == 0 ||
		group.DecodedBytes > limits.MaxDecodedBytes || group.CreatedAt.IsZero() {
		return fmt.Errorf("%w: write group accounting is invalid", ErrInvalidArgument)
	}
	var rows, decodedBytes uint64
	for _, member := range group.Members {
		if math.MaxUint64-rows < uint64(member.StoredRowCount) ||
			math.MaxUint64-decodedBytes < member.DecodedEventBytes {
			return fmt.Errorf("%w: write group accounting overflows", ErrInvalidArgument)
		}
		rows += uint64(member.StoredRowCount)
		decodedBytes += member.DecodedEventBytes
	}
	if rows != group.RowCount || decodedBytes != group.DecodedBytes ||
		group.FirstSequence != group.Members[0].Sequence ||
		group.LastSequence != group.Members[len(group.Members)-1].Sequence {
		return fmt.Errorf("write group aggregate accounting mismatch")
	}
	digest, err := WriteGroupMembershipSHA256(group.Members)
	if err != nil {
		return err
	}
	if digest != group.MembershipSHA256 {
		return errors.New("write group membership digest mismatch")
	}
	return nil
}

func validateWriteGroupLimits(limits WriteGroupLimits) error {
	if limits.TargetRows == 0 || limits.TargetRows > limits.MaxRows ||
		limits.TargetDecodedBytes == 0 || limits.TargetDecodedBytes > limits.MaxDecodedBytes ||
		limits.MaxRows < 1_000 || limits.MaxRows > MaxWriteGroupRows ||
		limits.MaxDecodedBytes < 8<<20 || limits.MaxDecodedBytes > MaxWriteGroupDecodedBytes ||
		limits.MaxMembers == 0 || limits.MaxMembers > MaxWriteGroupMembers ||
		limits.MaxLinger <= 0 {
		return fmt.Errorf("%w: write group limits are invalid", ErrInvalidArgument)
	}
	return nil
}

func validateWriteGroupID(groupID string) error {
	if groupID == "" || len(groupID) > MaxWriteGroupIDBytes || !utf8.ValidString(groupID) {
		return fmt.Errorf("%w: write group ID is invalid", ErrInvalidArgument)
	}
	return nil
}

func newWriteGroupID() (string, error) {
	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		return "", fmt.Errorf("generate write group ID: %w", err)
	}
	return "wg-" + hex.EncodeToString(random[:]), nil
}

// FormOrAcquireWriteGroup prioritizes the oldest ambiguous group, then the
// oldest ready group, and only then seals new ungrouped work. force seals a
// sparse group for an explicit drain without changing any hard bound.
func (sequencer *SQLiteSequencer) FormOrAcquireWriteGroup(
	ctx context.Context,
	attemptID string,
	limits WriteGroupLimits,
	force bool,
) (acquisition WriteGroupAcquisition, resultErr error) {
	if err := sequencer.beginOperation(); err != nil {
		return WriteGroupAcquisition{}, err
	}
	defer sequencer.endOperation()
	if err := validateAttemptID(ctx, attemptID); err != nil {
		return WriteGroupAcquisition{}, err
	}
	if err := validateWriteGroupLimits(limits); err != nil {
		return WriteGroupAcquisition{}, err
	}
	if !sequencer.leases.activate(attemptID) {
		return WriteGroupAcquisition{}, ErrAttemptInProgress
	}
	retainLease := false
	defer func() {
		if !retainLease {
			sequencer.leases.deactivate(attemptID)
		}
	}()

	tx, err := sequencer.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return WriteGroupAcquisition{}, fmt.Errorf("begin write-group acquisition: %w", err)
	}
	defer rollback(tx)

	groupID, state, priorOwner, found, err := selectRecoverableWriteGroup(ctx, tx)
	if err != nil {
		return WriteGroupAcquisition{}, err
	}
	if found {
		if priorOwner != "" && priorOwner != attemptID && sequencer.leases.contains(priorOwner) {
			if err := tx.Commit(); err != nil {
				return WriteGroupAcquisition{}, fmt.Errorf("commit busy write-group acquisition: %w", err)
			}
			return WriteGroupAcquisition{}, ErrAttemptInProgress
		}
		result, err := tx.ExecContext(ctx, `
			UPDATE ingest_write_groups SET attempt_id = ?
			WHERE write_group_id = ? AND state = ? AND attempt_id = ?`,
			attemptID, groupID, state, priorOwner)
		if err != nil {
			return WriteGroupAcquisition{}, fmt.Errorf("lease recoverable write group: %w", err)
		}
		if err := requireOneRow(result, "lease recoverable write group"); err != nil {
			return WriteGroupAcquisition{}, err
		}
		group, err := readWriteGroup(ctx, tx, groupID)
		if err != nil {
			return WriteGroupAcquisition{}, err
		}
		if err := ValidateWriteGroup(group, limits); err != nil {
			return WriteGroupAcquisition{}, fmt.Errorf("validate recoverable write group: %w", err)
		}
		if err := tx.Commit(); err != nil {
			return WriteGroupAcquisition{}, fmt.Errorf("commit recoverable write-group lease: %w", err)
		}
		sequencer.leases.bindGroup(attemptID, groupID)
		retainLease = true
		return WriteGroupAcquisition{
			Group:           group,
			Found:           true,
			FormationReason: WriteGroupFillRecovery,
		}, nil
	}

	selection, err := sequencer.selectWriteGroupCandidates(
		ctx,
		tx,
		limits,
		force,
		sequencer.currentTime(),
	)
	if err != nil {
		return WriteGroupAcquisition{}, err
	}
	if len(selection.candidates) == 0 || selection.reason == "" {
		if err := sequencer.commitRecoveredReservationLeases(tx, selection.recoveredOwners); err != nil {
			return WriteGroupAcquisition{}, fmt.Errorf("commit empty write-group acquisition: %w", err)
		}
		return WriteGroupAcquisition{NextLingerDeadline: selection.nextDeadline}, nil
	}
	members, err := sequencer.hydrateWriteGroupMembers(ctx, tx, selection.candidates)
	if err != nil {
		return WriteGroupAcquisition{}, err
	}
	groupID, err = newWriteGroupID()
	if err != nil {
		return WriteGroupAcquisition{}, err
	}
	membershipSHA256, err := WriteGroupMembershipSHA256(members)
	if err != nil {
		return WriteGroupAcquisition{}, fmt.Errorf("seal write-group membership: %w", err)
	}
	createdAt := sequencer.currentTime()
	if createdAt.Before(members[0].CreatedAt) {
		createdAt = members[0].CreatedAt
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO ingest_write_groups (
			write_group_id, state, attempt_id, member_count, row_count,
			decoded_bytes, membership_sha256, first_sequence, last_sequence,
			created_at_unix_micro, sending_at_unix_micro, committed_at_unix_micro
		) VALUES (?, 'ready', ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL)`,
		groupID,
		attemptID,
		len(members),
		selection.rows,
		selection.decodedBytes,
		membershipSHA256[:],
		members[0].Sequence,
		members[len(members)-1].Sequence,
		createdAt.UnixMicro(),
	); err != nil {
		return WriteGroupAcquisition{}, fmt.Errorf("persist write group: %w", err)
	}
	memberStatement, err := tx.PrepareContext(ctx, `
		INSERT INTO ingest_write_group_members (
			write_group_id, ordinal, visibility_sequence, row_count,
			decoded_bytes, outbox_sha256
		) VALUES (?, ?, ?, ?, ?, ?)`)
	if err != nil {
		return WriteGroupAcquisition{}, fmt.Errorf("prepare write-group membership: %w", err)
	}
	for ordinal, member := range members {
		if _, err := memberStatement.ExecContext(
			ctx,
			groupID,
			ordinal,
			member.Sequence,
			member.StoredRowCount,
			member.DecodedEventBytes,
			member.OutboxSHA256[:],
		); err != nil {
			_ = memberStatement.Close()
			return WriteGroupAcquisition{}, fmt.Errorf("persist write-group member %d: %w", ordinal, err)
		}
	}
	if err := memberStatement.Close(); err != nil {
		return WriteGroupAcquisition{}, fmt.Errorf("close write-group membership statement: %w", err)
	}
	group := WriteGroup{
		ID:               groupID,
		State:            WriteGroupReady,
		Members:          members,
		MemberCount:      safecast.MustConv[uint32](len(members)),
		RowCount:         selection.rows,
		DecodedBytes:     selection.decodedBytes,
		MembershipSHA256: membershipSHA256,
		FirstSequence:    members[0].Sequence,
		LastSequence:     members[len(members)-1].Sequence,
		CreatedAt:        createdAt,
	}
	if err := sequencer.commitRecoveredReservationLeases(tx, selection.recoveredOwners); err != nil {
		return WriteGroupAcquisition{}, fmt.Errorf("commit write-group formation: %w", err)
	}
	sequencer.leases.bindGroup(attemptID, groupID)
	retainLease = true
	return WriteGroupAcquisition{
		Group:           group,
		Found:           true,
		FormationReason: selection.reason,
	}, nil
}

func (sequencer *SQLiteSequencer) currentTime() time.Time {
	now := time.Now().Round(0).UTC()
	if sequencer.now != nil {
		now = sequencer.now().Round(0).UTC()
	}
	return now.Truncate(time.Microsecond)
}

func selectRecoverableWriteGroup(
	ctx context.Context,
	tx *sql.Tx,
) (string, WriteGroupState, string, bool, error) {
	var groupID, state, owner string
	err := tx.QueryRowContext(ctx, `
		SELECT write_group_id, state, attempt_id
		FROM ingest_write_groups
		WHERE state IN ('ready', 'ambiguous')
		ORDER BY CASE state WHEN 'ambiguous' THEN 0 ELSE 1 END, first_sequence
		LIMIT 1`).Scan(&groupID, &state, &owner)
	switch {
	case err == nil:
		return groupID, WriteGroupState(state), owner, true, nil
	case errors.Is(err, sql.ErrNoRows):
		return "", "", "", false, nil
	default:
		return "", "", "", false, fmt.Errorf("select recoverable write group: %w", err)
	}
}

type writeGroupCandidate struct {
	sequence     uint64
	owner        string
	rows         uint64
	decodedBytes uint64
	createdAt    time.Time
}

type writeGroupSelection struct {
	candidates      []writeGroupCandidate
	rows            uint64
	decodedBytes    uint64
	reason          WriteGroupFillReason
	nextDeadline    time.Time
	recoveredOwners map[string]struct{}
}

func (sequencer *SQLiteSequencer) selectWriteGroupCandidates(
	ctx context.Context,
	tx *sql.Tx,
	limits WriteGroupLimits,
	force bool,
	now time.Time,
) (writeGroupSelection, error) {
	rowsResult, err := tx.QueryContext(ctx, `
		SELECT reservation.sequence, reservation.attempt_id,
		       reservation.stored_row_count, reservation.decoded_event_bytes,
		       reservation.created_at_unix_micro
		FROM ingest_visibility_reservations AS reservation
		WHERE reservation.state = 'reserved'
		  AND reservation.phase = 'unsent'
		  AND NOT EXISTS (
		      SELECT 1 FROM ingest_write_group_members AS member
		      WHERE member.visibility_sequence = reservation.sequence
		  )
		ORDER BY reservation.sequence
		LIMIT ?`, MaxPendingReservations)
	if err != nil {
		return writeGroupSelection{}, fmt.Errorf("select ungrouped reservation accounting: %w", err)
	}
	rawCandidates := make([]writeGroupCandidate, 0, limits.MaxMembers)
	for rowsResult.Next() {
		var sequence, storedRows, decodedBytes, createdAtMicros int64
		var owner string
		if err := rowsResult.Scan(
			&sequence,
			&owner,
			&storedRows,
			&decodedBytes,
			&createdAtMicros,
		); err != nil {
			_ = rowsResult.Close()
			return writeGroupSelection{}, fmt.Errorf("scan ungrouped reservation accounting: %w", err)
		}
		decodedSequence, err := decodePositiveSequence(sequence)
		if err != nil {
			_ = rowsResult.Close()
			return writeGroupSelection{}, err
		}
		if storedRows < 1 || storedRows > math.MaxUint32 || decodedBytes < 1 || createdAtMicros < 1 {
			_ = rowsResult.Close()
			return writeGroupSelection{}, errors.New("pending reservation contains invalid write-group accounting")
		}
		rawCandidates = append(rawCandidates, writeGroupCandidate{
			sequence:     decodedSequence,
			owner:        owner,
			rows:         uint64(storedRows),
			decodedBytes: uint64(decodedBytes),
			createdAt:    time.UnixMicro(createdAtMicros).UTC(),
		})
	}
	if err := rowsResult.Err(); err != nil {
		_ = rowsResult.Close()
		return writeGroupSelection{}, fmt.Errorf("iterate ungrouped reservation accounting: %w", err)
	}
	if err := rowsResult.Close(); err != nil {
		return writeGroupSelection{}, fmt.Errorf("close ungrouped reservation accounting: %w", err)
	}

	selection := writeGroupSelection{
		candidates:      make([]writeGroupCandidate, 0, limits.MaxMembers),
		recoveredOwners: make(map[string]struct{}),
	}
	for _, candidate := range rawCandidates {
		if candidate.owner != "" && sequencer.leases.contains(candidate.owner) {
			continue
		}
		if candidate.rows > limits.MaxRows || candidate.decodedBytes > limits.MaxDecodedBytes {
			return writeGroupSelection{}, errors.New("pending reservation exceeds write-group hard bound")
		}
		if len(selection.candidates) == int(limits.MaxMembers) ||
			selection.rows > limits.MaxRows-candidate.rows ||
			selection.decodedBytes > limits.MaxDecodedBytes-candidate.decodedBytes {
			selection.reason = WriteGroupFillHardBoundary
			break
		}
		if candidate.owner != "" {
			result, err := tx.ExecContext(ctx, `
				UPDATE ingest_visibility_reservations
				SET attempt_id = ''
				WHERE sequence = ? AND state = 'reserved' AND phase = 'unsent'
				  AND attempt_id = ?
				  AND NOT EXISTS (
				      SELECT 1 FROM ingest_write_group_members AS member
				      WHERE member.visibility_sequence = ingest_visibility_reservations.sequence
				  )`, candidate.sequence, candidate.owner)
			if err != nil {
				return writeGroupSelection{}, fmt.Errorf("recover stale reservation lease: %w", err)
			}
			if err := requireOneRow(result, "recover stale reservation lease"); err != nil {
				return writeGroupSelection{}, err
			}
			selection.recoveredOwners[candidate.owner] = struct{}{}
		}
		selection.candidates = append(selection.candidates, candidate)
		selection.rows += candidate.rows
		selection.decodedBytes += candidate.decodedBytes
		switch {
		case selection.rows >= limits.TargetRows:
			selection.reason = WriteGroupFillRowTarget
		case selection.decodedBytes >= limits.TargetDecodedBytes:
			selection.reason = WriteGroupFillByteTarget
		case len(selection.candidates) == int(limits.MaxMembers):
			selection.reason = WriteGroupFillHardBoundary
		}
		if selection.reason != "" {
			break
		}
	}
	if len(selection.candidates) == 0 {
		return selection, nil
	}
	deadline := selection.candidates[0].createdAt.Add(limits.MaxLinger)
	if selection.reason == "" {
		switch {
		case force:
			selection.reason = WriteGroupFillDrain
		case !now.Before(deadline):
			selection.reason = WriteGroupFillLinger
		default:
			selection.nextDeadline = deadline
		}
	}
	return selection, nil
}

func (sequencer *SQLiteSequencer) hydrateWriteGroupMembers(
	ctx context.Context,
	tx *sql.Tx,
	candidates []writeGroupCandidate,
) ([]Reservation, error) {
	if sequencer.observeWriteGroupHydration != nil {
		sequencer.observeWriteGroupHydration()
	}
	rowsResult, err := tx.QueryContext(ctx, `
		SELECT reservation.sequence, identity.batch_key, identity.sequence_key,
		       reservation.state, reservation.phase, reservation.index_time_unix_milli,
		       identity.payload_sha256, reservation.metadata, reservation.outbox,
		       reservation.outbox_sha256, reservation.stored_row_count,
		       reservation.decoded_event_bytes, reservation.created_at_unix_micro,
		       reservation.committed_at_unix_micro
		FROM ingest_visibility_reservations AS reservation
		JOIN ingest_batch_identities AS identity ON identity.batch_key = reservation.batch_key
		WHERE reservation.state = 'reserved'
		  AND reservation.phase = 'unsent'
		  AND reservation.attempt_id = ''
		  AND NOT EXISTS (
		      SELECT 1 FROM ingest_write_group_members AS member
		      WHERE member.visibility_sequence = reservation.sequence
		  )
		ORDER BY reservation.sequence
		LIMIT ?`, len(candidates))
	if err != nil {
		return nil, fmt.Errorf("hydrate write-group members: %w", err)
	}
	defer rowsResult.Close()
	members := make([]Reservation, 0, len(candidates))
	for rowsResult.Next() {
		member, err := scanReservation(rowsResult)
		if err != nil {
			return nil, fmt.Errorf("scan write-group member: %w", err)
		}
		ordinal := len(members)
		candidate := candidates[ordinal]
		if member.Sequence != candidate.sequence || uint64(member.StoredRowCount) != candidate.rows ||
			member.DecodedEventBytes != candidate.decodedBytes || !member.CreatedAt.Equal(candidate.createdAt) {
			return nil, errors.New("write-group member changed during formation")
		}
		members = append(members, member)
	}
	if err := rowsResult.Err(); err != nil {
		return nil, fmt.Errorf("iterate hydrated write-group members: %w", err)
	}
	if len(members) != len(candidates) {
		return nil, errors.New("write-group member count changed during formation")
	}
	return members, nil
}

// commitRecoveredReservationLeases prevents an exact retry from reviving an
// owner between the live-owner check and the transaction commit. Reserve must
// activate its process lease before beginning its SQLite transaction, so this
// final check under the registry lock makes stale-owner recovery atomic with
// respect to every in-process reservation attempt.
func (sequencer *SQLiteSequencer) commitRecoveredReservationLeases(
	tx *sql.Tx,
	recoveredOwners map[string]struct{},
) error {
	if len(recoveredOwners) == 0 {
		return tx.Commit()
	}
	sequencer.leases.mu.Lock()
	defer sequencer.leases.mu.Unlock()
	for owner := range recoveredOwners {
		if _, live := sequencer.leases.active[owner]; live {
			return ErrAttemptInProgress
		}
	}
	return tx.Commit()
}

func readWriteGroup(ctx context.Context, q queryer, groupID string) (WriteGroup, error) {
	var group WriteGroup
	var state string
	var memberCount, rowCount, decodedBytes, firstSequence, lastSequence, createdAt int64
	var membershipSHA256 []byte
	var sendingAt, committedAt sql.NullInt64
	if err := q.QueryRowContext(ctx, `
		SELECT write_group_id, state, member_count, row_count, decoded_bytes,
		       membership_sha256, first_sequence, last_sequence,
		       created_at_unix_micro, sending_at_unix_micro, committed_at_unix_micro
		FROM ingest_write_groups WHERE write_group_id = ?`, groupID).Scan(
		&group.ID,
		&state,
		&memberCount,
		&rowCount,
		&decodedBytes,
		&membershipSHA256,
		&firstSequence,
		&lastSequence,
		&createdAt,
		&sendingAt,
		&committedAt,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return WriteGroup{}, ErrNotFound
		}
		return WriteGroup{}, fmt.Errorf("read write group: %w", err)
	}
	if memberCount < 1 || memberCount > MaxWriteGroupMembers || rowCount < 1 ||
		decodedBytes < 1 || firstSequence < 1 || lastSequence < firstSequence || createdAt < 1 {
		return WriteGroup{}, errors.New("write group contains invalid durable accounting")
	}
	group.State = WriteGroupState(state)
	group.MemberCount = safecast.MustConv[uint32](memberCount)
	group.RowCount = safecast.MustConv[uint64](rowCount)
	group.DecodedBytes = safecast.MustConv[uint64](decodedBytes)
	copy(group.MembershipSHA256[:], membershipSHA256)
	group.FirstSequence = safecast.MustConv[uint64](firstSequence)
	group.LastSequence = safecast.MustConv[uint64](lastSequence)
	group.CreatedAt = time.UnixMicro(createdAt).UTC()
	if sendingAt.Valid {
		group.SendingAt = time.UnixMicro(sendingAt.Int64).UTC()
	}
	if committedAt.Valid {
		group.CommittedAt = time.UnixMicro(committedAt.Int64).UTC()
	}

	memberRows, err := q.QueryContext(ctx, `
		SELECT reservation.sequence, identity.batch_key, identity.sequence_key,
		       reservation.state, reservation.phase, reservation.index_time_unix_milli,
		       identity.payload_sha256, reservation.metadata, reservation.outbox,
		       member.outbox_sha256, member.row_count, member.decoded_bytes,
		       reservation.created_at_unix_micro, reservation.committed_at_unix_micro
		FROM ingest_write_group_members AS member
		JOIN ingest_visibility_reservations AS reservation
		  ON reservation.sequence = member.visibility_sequence
		JOIN ingest_batch_identities AS identity ON identity.batch_key = reservation.batch_key
		WHERE member.write_group_id = ?
		ORDER BY member.ordinal`, groupID)
	if err != nil {
		return WriteGroup{}, fmt.Errorf("read write-group members: %w", err)
	}
	defer memberRows.Close()
	for memberRows.Next() {
		member, err := scanReservation(memberRows)
		if err != nil {
			return WriteGroup{}, fmt.Errorf("scan write-group member: %w", err)
		}
		group.Members = append(group.Members, member)
	}
	if err := memberRows.Err(); err != nil {
		return WriteGroup{}, fmt.Errorf("iterate write-group members: %w", err)
	}
	return group, nil
}

// MarkWriteGroupSending atomically marks a whole leased group ambiguous before
// the caller invokes the first ClickHouse side effect.
func (sequencer *SQLiteSequencer) MarkWriteGroupSending(
	ctx context.Context,
	groupID string,
	attemptID string,
) error {
	if err := sequencer.beginOperation(); err != nil {
		return err
	}
	defer sequencer.endOperation()
	if err := validateAttemptID(ctx, attemptID); err != nil {
		return err
	}
	if err := validateWriteGroupID(groupID); err != nil {
		return err
	}
	if !sequencer.leases.ownsGroup(attemptID, groupID) {
		return ErrAttemptLease
	}
	tx, err := sequencer.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return fmt.Errorf("begin write-group sending transition: %w", err)
	}
	defer rollback(tx)
	var state, owner string
	var firstSequence int64
	if err := tx.QueryRowContext(ctx, `
		SELECT state, attempt_id, first_sequence FROM ingest_write_groups
		WHERE write_group_id = ?`, groupID).Scan(&state, &owner, &firstSequence); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNotFound
		}
		return fmt.Errorf("read write group before sending: %w", err)
	}
	if owner != attemptID || (state != string(WriteGroupReady) && state != string(WriteGroupAmbiguous)) {
		return ErrAttemptLease
	}
	var barrier int
	if err := tx.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM ingest_write_groups
			WHERE state = 'ambiguous' AND write_group_id <> ?
			  AND (? = 'ready' OR first_sequence < ?)
		)`, groupID, state, firstSequence).Scan(&barrier); err != nil {
		return fmt.Errorf("read write-group ambiguous barrier: %w", err)
	}
	if barrier != 0 {
		return ErrAmbiguousBarrier
	}
	group, err := readWriteGroup(ctx, tx, groupID)
	if err != nil {
		return err
	}
	globalLimits := WriteGroupLimits{
		TargetRows:         1,
		TargetDecodedBytes: 1,
		MaxRows:            MaxWriteGroupRows,
		MaxDecodedBytes:    MaxWriteGroupDecodedBytes,
		MaxMembers:         MaxWriteGroupMembers,
		MaxLinger:          time.Microsecond,
	}
	if err := ValidateWriteGroup(group, globalLimits); err != nil {
		return fmt.Errorf("validate write group before sending: %w", err)
	}
	wantPhase := phaseUnsent
	if state == string(WriteGroupAmbiguous) {
		wantPhase = phaseAmbiguous
	}
	result, err := tx.ExecContext(ctx, `
		UPDATE ingest_visibility_reservations
		SET phase = 'ambiguous'
		WHERE state = 'reserved' AND phase = ? AND attempt_id = ''
		  AND sequence IN (
		      SELECT visibility_sequence FROM ingest_write_group_members
		      WHERE write_group_id = ?
		  )`, wantPhase, groupID)
	if err != nil {
		return fmt.Errorf("mark write-group members ambiguous: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("mark write-group members ambiguous: read affected rows: %w", err)
	}
	if changed != int64(group.MemberCount) {
		return fmt.Errorf(
			"mark write-group members ambiguous: changed %d of %d",
			changed,
			group.MemberCount,
		)
	}
	sendingAt := sequencer.currentTime()
	if sendingAt.Before(group.CreatedAt) {
		sendingAt = group.CreatedAt
	}
	result, err = tx.ExecContext(ctx, `
		UPDATE ingest_write_groups
		SET state = 'ambiguous', sending_at_unix_micro = COALESCE(sending_at_unix_micro, ?)
		WHERE write_group_id = ? AND state = ? AND attempt_id = ?`,
		sendingAt.UnixMicro(), groupID, state, attemptID)
	if err != nil {
		return fmt.Errorf("mark write group ambiguous: %w", err)
	}
	if err := requireOneRow(result, "mark write group ambiguous"); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit write-group sending transition: %w", err)
	}
	return nil
}

// CommitWriteGroup atomically commits all logical members, clears their
// outboxes, fires reservation-backed HEC transitions, marks the group terminal,
// and advances the visibility cutoff once.
func (sequencer *SQLiteSequencer) CommitWriteGroup(
	ctx context.Context,
	groupID string,
	attemptID string,
	committedAt time.Time,
) ([]uint64, error) {
	if err := sequencer.beginOperation(); err != nil {
		return nil, err
	}
	defer sequencer.endOperation()
	if err := validateAttemptID(ctx, attemptID); err != nil {
		return nil, err
	}
	if err := validateWriteGroupID(groupID); err != nil {
		return nil, err
	}
	if committedAt.IsZero() {
		return nil, fmt.Errorf("%w: committed time is required", ErrInvalidArgument)
	}
	committedAt = committedAt.Round(0).UTC().Truncate(time.Microsecond)
	if committedAt.UnixMicro() < 1 {
		return nil, fmt.Errorf("%w: committed time is outside persistent bounds", ErrInvalidArgument)
	}
	if sequencer.leases.ownsGroup(attemptID, groupID) {
		defer sequencer.leases.deactivate(attemptID)
	}
	tx, err := sequencer.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return nil, fmt.Errorf("begin write-group commit: %w", err)
	}
	defer rollback(tx)
	var state, owner string
	var sendingAt sql.NullInt64
	if err := tx.QueryRowContext(ctx, `
		SELECT state, attempt_id, sending_at_unix_micro
		FROM ingest_write_groups WHERE write_group_id = ?`, groupID).Scan(
		&state,
		&owner,
		&sendingAt,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("read write group before commit: %w", err)
	}
	sequences, err := readWriteGroupSequences(ctx, tx, groupID)
	if err != nil {
		return nil, err
	}
	if state == string(WriteGroupCommitted) {
		if err := tx.Commit(); err != nil {
			return nil, fmt.Errorf("commit terminal write-group lookup: %w", err)
		}
		return sequences, nil
	}
	if state != string(WriteGroupAmbiguous) || owner != attemptID ||
		!sequencer.leases.ownsGroup(attemptID, groupID) || !sendingAt.Valid {
		return nil, ErrAttemptLease
	}
	if committedAt.UnixMicro() < sendingAt.Int64 {
		committedAt = time.UnixMicro(sendingAt.Int64).UTC()
	}
	statement, err := tx.PrepareContext(ctx, `
		UPDATE ingest_visibility_reservations
		SET state = 'committed', phase = 'final', attempt_id = '', outbox = X'',
		    outbox_sha256 = X'', stored_row_count = 0, decoded_event_bytes = 0,
		    committed_at_unix_micro = ?
		WHERE sequence = ? AND state = 'reserved' AND phase = 'ambiguous'
		  AND attempt_id = ''`)
	if err != nil {
		return nil, fmt.Errorf("prepare write-group member commit: %w", err)
	}
	for _, sequence := range sequences {
		result, err := statement.ExecContext(ctx, committedAt.UnixMicro(), sequence)
		if err != nil {
			_ = statement.Close()
			return nil, fmt.Errorf("commit write-group member %d: %w", sequence, err)
		}
		if err := requireOneRow(result, "commit write-group member"); err != nil {
			_ = statement.Close()
			return nil, err
		}
	}
	if err := statement.Close(); err != nil {
		return nil, fmt.Errorf("close write-group member commit statement: %w", err)
	}
	result, err := tx.ExecContext(ctx, `
		UPDATE ingest_write_groups
		SET state = 'committed', attempt_id = '', committed_at_unix_micro = ?
		WHERE write_group_id = ? AND state = 'ambiguous' AND attempt_id = ?`,
		committedAt.UnixMicro(), groupID, attemptID)
	if err != nil {
		return nil, fmt.Errorf("mark write group committed: %w", err)
	}
	if err := requireOneRow(result, "mark write group committed"); err != nil {
		return nil, err
	}
	if err := advanceCutoff(ctx, tx); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit write-group finalization: %w", err)
	}
	return sequences, nil
}

func readWriteGroupSequences(ctx context.Context, q queryer, groupID string) ([]uint64, error) {
	rows, err := q.QueryContext(ctx, `
		SELECT visibility_sequence FROM ingest_write_group_members
		WHERE write_group_id = ? ORDER BY ordinal`, groupID)
	if err != nil {
		return nil, fmt.Errorf("read write-group member sequences: %w", err)
	}
	defer rows.Close()
	var sequences []uint64
	for rows.Next() {
		var sequence int64
		if err := rows.Scan(&sequence); err != nil {
			return nil, fmt.Errorf("scan write-group member sequence: %w", err)
		}
		decoded, err := decodePositiveSequence(sequence)
		if err != nil {
			return nil, err
		}
		sequences = append(sequences, decoded)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate write-group member sequences: %w", err)
	}
	if len(sequences) == 0 || len(sequences) > MaxWriteGroupMembers {
		return nil, errors.New("write group contains invalid member count")
	}
	return sequences, nil
}

// ReleaseWriteGroup clears only the live process lease. Ready membership and
// ambiguous replay authority remain durable and immutable.
func (sequencer *SQLiteSequencer) ReleaseWriteGroup(
	ctx context.Context,
	groupID string,
	attemptID string,
) error {
	if err := sequencer.beginOperation(); err != nil {
		return err
	}
	defer sequencer.endOperation()
	if err := validateAttemptID(ctx, attemptID); err != nil {
		return err
	}
	if err := validateWriteGroupID(groupID); err != nil {
		return err
	}
	owned := sequencer.leases.ownsGroup(attemptID, groupID)
	if owned {
		defer sequencer.leases.deactivate(attemptID)
	}
	tx, err := sequencer.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return fmt.Errorf("begin write-group release: %w", err)
	}
	defer rollback(tx)
	var state, owner string
	if err := tx.QueryRowContext(ctx, `
		SELECT state, attempt_id FROM ingest_write_groups WHERE write_group_id = ?`, groupID).Scan(
		&state,
		&owner,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNotFound
		}
		return fmt.Errorf("read write group before release: %w", err)
	}
	if state == string(WriteGroupCommitted) {
		return tx.Commit()
	}
	if !owned || owner != attemptID {
		return ErrAttemptLease
	}
	result, err := tx.ExecContext(ctx, `
		UPDATE ingest_write_groups SET attempt_id = ''
		WHERE write_group_id = ? AND state IN ('ready', 'ambiguous') AND attempt_id = ?`,
		groupID, attemptID)
	if err != nil {
		return fmt.Errorf("release write-group lease: %w", err)
	}
	if err := requireOneRow(result, "release write-group lease"); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit write-group release: %w", err)
	}
	return nil
}
