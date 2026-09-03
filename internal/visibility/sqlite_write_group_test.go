package visibility

import (
	"context"
	"crypto/sha256"
	"errors"
	"slices"
	"testing"
	"time"
)

func testWriteGroupLimits() WriteGroupLimits {
	return WriteGroupLimits{
		TargetRows:         1_000,
		TargetDecodedBytes: 16 << 20,
		MaxRows:            MaxWriteGroupRows,
		MaxDecodedBytes:    MaxWriteGroupDecodedBytes,
		MaxMembers:         MaxWriteGroupMembers,
		MaxLinger:          200 * time.Millisecond,
	}
}

func stageGroupMember(
	t *testing.T,
	sequencer *SQLiteSequencer,
	key string,
	rows uint32,
	decodedBytes uint64,
) Reservation {
	t.Helper()
	request := reserveRequest(key, "stage-"+key)
	request.StoredRowCount = rows
	request.DecodedEventBytes = decodedBytes
	reservation, err := sequencer.Reserve(context.Background(), request)
	if err != nil {
		t.Fatalf("Reserve(%q): %v", key, err)
	}
	if err := sequencer.Release(context.Background(), reservation.Sequence, request.AttemptID); err != nil {
		t.Fatalf("Release(%q): %v", key, err)
	}
	return reservation
}

func TestSQLiteWriteGroupFormsAtRowTargetAndCommitsAtomically(t *testing.T) {
	t.Parallel()
	sequencer, db := openTestSequencer(t)
	createdAt := testCommittedAt.Add(-time.Hour)
	sequencer.now = func() time.Time { return createdAt }
	first := stageGroupMember(t, sequencer, "group-row-one", 400, 100)
	second := stageGroupMember(t, sequencer, "group-row-two", 400, 200)
	third := stageGroupMember(t, sequencer, "group-row-three", 400, 300)

	acquired, err := sequencer.FormOrAcquireWriteGroup(
		context.Background(), "group-owner", testWriteGroupLimits(), false,
	)
	if err != nil {
		t.Fatal(err)
	}
	group := acquired.Group
	if !acquired.Found || acquired.FormationReason != WriteGroupFillRowTarget ||
		group.State != WriteGroupReady || group.MemberCount != 3 ||
		group.RowCount != 1_200 || group.DecodedBytes != 600 ||
		group.FirstSequence != first.Sequence || group.LastSequence != third.Sequence {
		t.Fatalf("formed write group = %+v", acquired)
	}
	if got := []uint64{
		group.Members[0].Sequence,
		group.Members[1].Sequence,
		group.Members[2].Sequence,
	}; !slices.Equal(got, []uint64{first.Sequence, second.Sequence, third.Sequence}) {
		t.Fatalf("member sequence order = %v", got)
	}
	if err := ValidateWriteGroup(group, testWriteGroupLimits()); err != nil {
		t.Fatalf("ValidateWriteGroup: %v", err)
	}
	if pending, found, err := sequencer.AcquirePending(
		context.Background(), "legacy-reservation-owner",
	); err != nil || found {
		t.Fatalf("legacy AcquirePending reached grouped work: %+v found=%v error=%v", pending, found, err)
	}
	usage, err := sequencer.PendingUsage(context.Background())
	if err != nil || usage.Reservations != 3 || usage.UngroupedReservations != 0 ||
		usage.ReadyGroups != 1 || usage.AmbiguousGroups != 0 || usage.LeasedGroups != 1 ||
		!usage.OldestPendingAt.Equal(createdAt) {
		t.Fatalf("ready PendingUsage = %+v error=%v", usage, err)
	}

	sequencer.now = func() time.Time { return testCommittedAt.Add(-time.Second) }
	if err := sequencer.MarkWriteGroupSending(context.Background(), group.ID, "group-owner"); err != nil {
		t.Fatal(err)
	}
	usage, err = sequencer.PendingUsage(context.Background())
	if err != nil || usage.ReadyGroups != 0 || usage.AmbiguousGroups != 1 {
		t.Fatalf("ambiguous PendingUsage = %+v error=%v", usage, err)
	}
	committed, err := sequencer.CommitWriteGroup(
		context.Background(), group.ID, "group-owner", testCommittedAt,
	)
	if err != nil || !slices.Equal(committed, []uint64{1, 2, 3}) {
		t.Fatalf("CommitWriteGroup = %v, %v", committed, err)
	}
	assertCutoff(t, sequencer, third.Sequence)
	usage, err = sequencer.PendingUsage(context.Background())
	if err != nil || usage != (PendingUsage{}) {
		t.Fatalf("terminal PendingUsage = %+v error=%v", usage, err)
	}
	var committedMembers, uncleared int
	if err := db.SQLDB().QueryRowContext(t.Context(), `
		SELECT count(*), count(*) FILTER (
			WHERE length(outbox) <> 0 OR length(outbox_sha256) <> 0
			   OR stored_row_count <> 0 OR decoded_event_bytes <> 0
		)
		FROM ingest_visibility_reservations
		WHERE state = 'committed'`).Scan(&committedMembers, &uncleared); err != nil {
		t.Fatal(err)
	}
	if committedMembers != 3 || uncleared != 0 {
		t.Fatalf("committed members=%d uncleared=%d", committedMembers, uncleared)
	}
	var groupState, owner string
	if err := db.SQLDB().QueryRowContext(t.Context(), `
		SELECT state, attempt_id FROM ingest_write_groups WHERE write_group_id = ?`,
		group.ID,
	).Scan(&groupState, &owner); err != nil {
		t.Fatal(err)
	}
	if groupState != string(WriteGroupCommitted) || owner != "" {
		t.Fatalf("terminal group state=%q owner=%q", groupState, owner)
	}
}

func TestSQLiteWriteGroupSparseLingerUsesDurableCreationTime(t *testing.T) {
	t.Parallel()
	sequencer, _ := openTestSequencer(t)
	createdAt := testCommittedAt.Add(-time.Hour)
	observedAt := createdAt
	sequencer.now = func() time.Time { return observedAt }
	stageGroupMember(t, sequencer, "group-linger", 1, 1)

	observedAt = createdAt.Add(199 * time.Millisecond)
	acquired, err := sequencer.FormOrAcquireWriteGroup(
		context.Background(), "linger-before", testWriteGroupLimits(), false,
	)
	if err != nil || acquired.Found ||
		!acquired.NextLingerDeadline.Equal(createdAt.Add(200*time.Millisecond)) {
		t.Fatalf("pre-linger acquisition = %+v error=%v", acquired, err)
	}
	observedAt = createdAt.Add(200 * time.Millisecond)
	acquired, err = sequencer.FormOrAcquireWriteGroup(
		context.Background(), "linger-at", testWriteGroupLimits(), false,
	)
	if err != nil || !acquired.Found || acquired.FormationReason != WriteGroupFillLinger ||
		acquired.Group.MemberCount != 1 {
		t.Fatalf("at-linger acquisition = %+v error=%v", acquired, err)
	}
}

func TestSQLiteWriteGroupSubtargetProbesDoNotHydrateOutboxes(t *testing.T) {
	t.Parallel()
	sequencer, _ := openTestSequencer(t)
	createdAt := testCommittedAt.Add(-time.Hour)
	sequencer.now = func() time.Time { return createdAt }
	stageGroupMember(t, sequencer, "group-no-early-hydration", 1, 1)

	hydrations := 0
	sequencer.observeWriteGroupHydration = func() { hydrations++ }
	sequencer.now = func() time.Time { return createdAt.Add(100 * time.Millisecond) }
	for _, attemptID := range []string{"probe-one", "probe-two", "probe-three"} {
		acquired, err := sequencer.FormOrAcquireWriteGroup(
			context.Background(), attemptID, testWriteGroupLimits(), false,
		)
		if err != nil || acquired.Found || acquired.NextLingerDeadline.IsZero() {
			t.Fatalf("sub-target acquisition = %+v error=%v", acquired, err)
		}
	}
	if hydrations != 0 {
		t.Fatalf("sub-target probes hydrated replay outboxes %d times", hydrations)
	}

	acquired, err := sequencer.FormOrAcquireWriteGroup(
		context.Background(), "forced-hydration", testWriteGroupLimits(), true,
	)
	if err != nil || !acquired.Found {
		t.Fatalf("forced acquisition = %+v error=%v", acquired, err)
	}
	if hydrations != 1 {
		t.Fatalf("sealed group hydration count = %d, want 1", hydrations)
	}
}

func TestSQLiteWriteGroupRecoversOutcomeAmbiguousReserveWithoutRestart(t *testing.T) {
	t.Parallel()
	sequencer, db := openTestSequencer(t)
	request := reserveRequest("group-ambiguous-reserve", "reservation-owner")
	request.StoredRowCount = 1
	request.DecodedEventBytes = 1
	reservation, err := sequencer.Reserve(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	// Reproduce the state after SQLite commits Reserve but Commit reports an
	// error: the deferred failure path drops the process lease before bind,
	// while the durable attempt ID remains authoritative but ownerless.
	sequencer.leases.deactivate(request.AttemptID)

	acquired, err := sequencer.FormOrAcquireWriteGroup(
		context.Background(), "group-recovery-owner", testWriteGroupLimits(), true,
	)
	if err != nil || !acquired.Found || acquired.Group.MemberCount != 1 ||
		acquired.Group.Members[0].Sequence != reservation.Sequence {
		t.Fatalf("recovered acquisition = %+v error=%v", acquired, err)
	}
	var durableOwner string
	if err := db.SQLDB().QueryRowContext(t.Context(), `
		SELECT attempt_id FROM ingest_visibility_reservations WHERE sequence = ?`,
		reservation.Sequence,
	).Scan(&durableOwner); err != nil {
		t.Fatal(err)
	}
	if durableOwner != "" {
		t.Fatalf("recovered reservation owner = %q, want empty", durableOwner)
	}
}

func TestSQLiteWriteGroupRecoversFailedReleaseWithoutRestart(t *testing.T) {
	t.Parallel()
	sequencer, db := openTestSequencer(t)
	request := reserveRequest("group-failed-release", "failed-release-owner")
	request.StoredRowCount = 1
	request.DecodedEventBytes = 1
	reservation, err := sequencer.Reserve(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := sequencer.Release(canceled, reservation.Sequence, request.AttemptID); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Release error = %v, want context.Canceled", err)
	}
	if sequencer.leases.contains(request.AttemptID) {
		t.Fatal("failed Release retained its process lease")
	}
	var durableOwner string
	if err := db.SQLDB().QueryRowContext(t.Context(), `
		SELECT attempt_id FROM ingest_visibility_reservations WHERE sequence = ?`,
		reservation.Sequence,
	).Scan(&durableOwner); err != nil {
		t.Fatal(err)
	}
	if durableOwner != request.AttemptID {
		t.Fatalf("owner after failed Release = %q, want %q", durableOwner, request.AttemptID)
	}

	acquired, err := sequencer.FormOrAcquireWriteGroup(
		context.Background(), "failed-release-recovery", testWriteGroupLimits(), true,
	)
	if err != nil || !acquired.Found || acquired.Group.MemberCount != 1 ||
		acquired.Group.Members[0].Sequence != reservation.Sequence {
		t.Fatalf("recovered acquisition = %+v error=%v", acquired, err)
	}
}

func TestSQLiteWriteGroupDoesNotStealLiveReservationAttempt(t *testing.T) {
	t.Parallel()
	sequencer, db := openTestSequencer(t)
	request := reserveRequest("group-live-reservation", "live-reservation-owner")
	request.StoredRowCount = 1_000
	reservation, err := sequencer.Reserve(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}

	acquired, err := sequencer.FormOrAcquireWriteGroup(
		context.Background(), "group-competing-owner", testWriteGroupLimits(), true,
	)
	if err != nil || acquired.Found {
		t.Fatalf("acquisition over live reservation = %+v error=%v", acquired, err)
	}
	var durableOwner string
	if err := db.SQLDB().QueryRowContext(t.Context(), `
		SELECT attempt_id FROM ingest_visibility_reservations WHERE sequence = ?`,
		reservation.Sequence,
	).Scan(&durableOwner); err != nil {
		t.Fatal(err)
	}
	if durableOwner != request.AttemptID || !sequencer.leases.owns(request.AttemptID, reservation.Sequence) {
		t.Fatalf("live reservation was stolen: durable=%q", durableOwner)
	}

	if err := sequencer.Release(context.Background(), reservation.Sequence, request.AttemptID); err != nil {
		t.Fatal(err)
	}
	acquired, err = sequencer.FormOrAcquireWriteGroup(
		context.Background(), "group-after-release", testWriteGroupLimits(), true,
	)
	if err != nil || !acquired.Found || acquired.Group.Members[0].Sequence != reservation.Sequence {
		t.Fatalf("acquisition after Release = %+v error=%v", acquired, err)
	}
}

func TestSQLiteWriteGroupStaleRecoveryDoesNotRaceRevivedReservationAttempt(t *testing.T) {
	t.Parallel()
	sequencer, db := openTestSequencer(t)
	request := reserveRequest("group-revived-reservation", "revived-reservation-owner")
	request.StoredRowCount = 1_000
	reservation, err := sequencer.Reserve(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	sequencer.leases.deactivate(request.AttemptID)
	sequencer.observeWriteGroupHydration = func() {
		if !sequencer.leases.activate(request.AttemptID) {
			t.Error("failed to revive reservation attempt during formation")
		}
	}

	if _, err := sequencer.FormOrAcquireWriteGroup(
		context.Background(), "group-racing-owner", testWriteGroupLimits(), true,
	); !errors.Is(err, ErrAttemptInProgress) {
		t.Fatalf("racing stale-owner recovery error = %v, want ErrAttemptInProgress", err)
	}
	var durableOwner string
	if err := db.SQLDB().QueryRowContext(t.Context(), `
		SELECT attempt_id FROM ingest_visibility_reservations WHERE sequence = ?`,
		reservation.Sequence,
	).Scan(&durableOwner); err != nil {
		t.Fatal(err)
	}
	if durableOwner != request.AttemptID || !sequencer.leases.contains(request.AttemptID) {
		t.Fatalf("revived reservation was stolen: durable=%q", durableOwner)
	}
	var groups int
	if err := db.SQLDB().QueryRowContext(
		t.Context(),
		`SELECT count(*) FROM ingest_write_groups`,
	).Scan(&groups); err != nil {
		t.Fatal(err)
	}
	if groups != 0 {
		t.Fatalf("racing recovery persisted %d write groups", groups)
	}
	sequencer.leases.deactivate(request.AttemptID)
}

func TestSQLiteWriteGroupMemberCannotBeReacquiredByReservationAttempt(t *testing.T) {
	t.Parallel()
	sequencer, _ := openTestSequencer(t)
	member := stageGroupMember(t, sequencer, "group-member-reacquire", 1_000, 100)
	acquired, err := sequencer.FormOrAcquireWriteGroup(
		context.Background(), "group-member-owner", testWriteGroupLimits(), false,
	)
	if err != nil || !acquired.Found {
		t.Fatalf("group acquisition = %+v error=%v", acquired, err)
	}
	retry := ReserveRequest{
		BatchKey:      member.BatchKey,
		SequenceKey:   member.SequenceKey,
		AttemptID:     "reservation-retry-owner",
		ExistingOnly:  true,
		PayloadSHA256: member.PayloadSHA256,
	}
	if _, err := sequencer.Reserve(context.Background(), retry); !errors.Is(err, ErrAttemptInProgress) {
		t.Fatalf("grouped ExistingOnly Reserve error = %v, want ErrAttemptInProgress", err)
	}
	if !sequencer.leases.ownsGroup("group-member-owner", acquired.Group.ID) {
		t.Fatal("reservation retry disturbed the live group lease")
	}
}

func TestSQLiteWriteGroupHardBoundaryDoesNotSplitLogicalBatch(t *testing.T) {
	t.Parallel()
	sequencer, _ := openTestSequencer(t)
	stageGroupMember(t, sequencer, "group-hard-one", 600, 100)
	stageGroupMember(t, sequencer, "group-hard-two", 600, 100)
	limits := testWriteGroupLimits()
	limits.TargetRows = 1_000
	limits.TargetDecodedBytes = 64 << 20
	limits.MaxRows = 1_000
	acquired, err := sequencer.FormOrAcquireWriteGroup(
		context.Background(), "hard-owner", limits, false,
	)
	if err != nil || !acquired.Found || acquired.FormationReason != WriteGroupFillHardBoundary ||
		acquired.Group.MemberCount != 1 || acquired.Group.RowCount != 600 {
		t.Fatalf("hard-boundary acquisition = %+v error=%v", acquired, err)
	}
	usage, err := sequencer.PendingUsage(context.Background())
	if err != nil || usage.UngroupedReservations != 1 || usage.ReadyGroups != 1 {
		t.Fatalf("hard-boundary PendingUsage = %+v error=%v", usage, err)
	}
}

func TestSQLiteWriteGroupLeaseFencingAndRecovery(t *testing.T) {
	t.Parallel()
	sequencer, db := openTestSequencer(t)
	stageGroupMember(t, sequencer, "group-lease", 1, 1)
	first, err := sequencer.FormOrAcquireWriteGroup(
		context.Background(), "first-group-owner", testWriteGroupLimits(), true,
	)
	if err != nil || !first.Found {
		t.Fatalf("first acquisition = %+v error=%v", first, err)
	}
	if _, err := sequencer.FormOrAcquireWriteGroup(
		context.Background(), "competing-group-owner", testWriteGroupLimits(), true,
	); !errors.Is(err, ErrAttemptInProgress) {
		t.Fatalf("competing acquisition error = %v, want ErrAttemptInProgress", err)
	}
	if err := sequencer.MarkWriteGroupSending(
		context.Background(), first.Group.ID, "wrong-owner",
	); !errors.Is(err, ErrAttemptLease) {
		t.Fatalf("wrong-owner MarkWriteGroupSending error = %v", err)
	}
	if err := sequencer.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := NewSQLite(context.Background(), db)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	recovered, err := reopened.FormOrAcquireWriteGroup(
		context.Background(), "restart-owner", testWriteGroupLimits(), false,
	)
	if err != nil || !recovered.Found || recovered.FormationReason != WriteGroupFillRecovery ||
		recovered.Group.ID != first.Group.ID ||
		recovered.Group.MembershipSHA256 != first.Group.MembershipSHA256 {
		t.Fatalf("restart acquisition = %+v error=%v", recovered, err)
	}
}

func TestSQLiteWriteGroupAmbiguousRecoveryPrecedesNewerReadyGroup(t *testing.T) {
	t.Parallel()
	sequencer, db := openTestSequencer(t)
	sequencer.now = func() time.Time { return testCommittedAt.Add(-time.Hour) }
	stageGroupMember(t, sequencer, "group-priority-one", 1_000, 100)
	second := stageGroupMember(t, sequencer, "group-priority-two", 1_000, 100)
	first, err := sequencer.FormOrAcquireWriteGroup(
		context.Background(), "priority-first-owner", testWriteGroupLimits(), false,
	)
	if err != nil {
		t.Fatal(err)
	}
	secondDigest, err := WriteGroupMembershipSHA256([]Reservation{second})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.SQLDB().ExecContext(t.Context(), `
		INSERT INTO ingest_write_groups (
			write_group_id, state, attempt_id, member_count, row_count,
			decoded_bytes, membership_sha256, first_sequence, last_sequence,
			created_at_unix_micro, sending_at_unix_micro, committed_at_unix_micro
		) VALUES ('second-ready', 'ready', '', 1, ?, ?, ?, ?, ?, ?, NULL, NULL)`,
		second.StoredRowCount,
		second.DecodedEventBytes,
		secondDigest[:],
		second.Sequence,
		second.Sequence,
		testCommittedAt.Add(-time.Hour).UnixMicro(),
	); err != nil {
		t.Fatal(err)
	}
	if _, err := db.SQLDB().ExecContext(t.Context(), `
		INSERT INTO ingest_write_group_members (
			write_group_id, ordinal, visibility_sequence, row_count,
			decoded_bytes, outbox_sha256
		) VALUES ('second-ready', 0, ?, ?, ?, ?)`,
		second.Sequence,
		second.StoredRowCount,
		second.DecodedEventBytes,
		second.OutboxSHA256[:],
	); err != nil {
		t.Fatal(err)
	}
	sequencer.now = func() time.Time { return testCommittedAt.Add(-time.Second) }
	if err := sequencer.MarkWriteGroupSending(
		context.Background(), first.Group.ID, "priority-first-owner",
	); err != nil {
		t.Fatal(err)
	}
	if err := sequencer.ReleaseWriteGroup(
		context.Background(), first.Group.ID, "priority-first-owner",
	); err != nil {
		t.Fatal(err)
	}
	recovered, err := sequencer.FormOrAcquireWriteGroup(
		context.Background(), "priority-recovery-owner", testWriteGroupLimits(), false,
	)
	if err != nil || !recovered.Found || recovered.Group.ID != first.Group.ID ||
		recovered.Group.State != WriteGroupAmbiguous ||
		recovered.FormationReason != WriteGroupFillRecovery {
		t.Fatalf("priority recovery = %+v error=%v", recovered, err)
	}
}

func TestSQLiteWriteGroupSchemaRejectsMembershipMutationAndIllegalState(t *testing.T) {
	t.Parallel()
	sequencer, db := openTestSequencer(t)
	stageGroupMember(t, sequencer, "group-schema", 1_000, 100)
	acquired, err := sequencer.FormOrAcquireWriteGroup(
		context.Background(), "schema-owner", testWriteGroupLimits(), false,
	)
	if err != nil {
		t.Fatal(err)
	}
	for name, statement := range map[string]string{
		"member update":    `UPDATE ingest_write_group_members SET row_count = row_count + 1`,
		"member delete":    `DELETE FROM ingest_write_group_members`,
		"aggregate update": `UPDATE ingest_write_groups SET row_count = row_count + 1`,
		"illegal state": `UPDATE ingest_write_groups
			SET state = 'committed', attempt_id = '', sending_at_unix_micro = created_at_unix_micro,
			    committed_at_unix_micro = created_at_unix_micro`,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := db.SQLDB().ExecContext(t.Context(), statement); err == nil {
				t.Fatalf("schema accepted %s for group %q", name, acquired.Group.ID)
			}
		})
	}
}

func TestSQLiteWriteGroupMarkSendingRollsBackPartialTransition(t *testing.T) {
	t.Parallel()
	sequencer, db := openTestSequencer(t)
	stageGroupMember(t, sequencer, "group-mark-one", 500, 100)
	stageGroupMember(t, sequencer, "group-mark-two", 500, 100)
	acquired, err := sequencer.FormOrAcquireWriteGroup(
		context.Background(), "mark-owner", testWriteGroupLimits(), false,
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.SQLDB().ExecContext(t.Context(), `
		UPDATE ingest_visibility_reservations SET attempt_id = 'poison'
		WHERE sequence = ?`, acquired.Group.LastSequence); err != nil {
		t.Fatal(err)
	}
	if err := sequencer.MarkWriteGroupSending(
		context.Background(), acquired.Group.ID, "mark-owner",
	); err == nil {
		t.Fatal("MarkWriteGroupSending succeeded with a poisoned member")
	}
	var ambiguousMembers int
	if err := db.SQLDB().QueryRowContext(t.Context(), `
		SELECT count(*) FROM ingest_visibility_reservations
		WHERE phase = 'ambiguous'`).Scan(&ambiguousMembers); err != nil {
		t.Fatal(err)
	}
	if ambiguousMembers != 0 {
		t.Fatalf("ambiguous members after rollback = %d", ambiguousMembers)
	}
	var state string
	if err := db.SQLDB().QueryRowContext(t.Context(), `
		SELECT state FROM ingest_write_groups WHERE write_group_id = ?`,
		acquired.Group.ID,
	).Scan(&state); err != nil {
		t.Fatal(err)
	}
	if state != string(WriteGroupReady) {
		t.Fatalf("group state after rollback = %q", state)
	}
}

func TestSQLiteWriteGroupCommitRollsBackEveryMember(t *testing.T) {
	t.Parallel()
	sequencer, db := openTestSequencer(t)
	stageGroupMember(t, sequencer, "group-commit-one", 500, 100)
	stageGroupMember(t, sequencer, "group-commit-two", 500, 100)
	acquired, err := sequencer.FormOrAcquireWriteGroup(
		context.Background(), "commit-owner", testWriteGroupLimits(), false,
	)
	if err != nil {
		t.Fatal(err)
	}
	sequencer.now = func() time.Time { return testCommittedAt.Add(-time.Second) }
	if err := sequencer.MarkWriteGroupSending(
		context.Background(), acquired.Group.ID, "commit-owner",
	); err != nil {
		t.Fatal(err)
	}
	if _, err := db.SQLDB().ExecContext(t.Context(), `
		UPDATE ingest_visibility_reservations SET attempt_id = 'poison'
		WHERE sequence = ?`, acquired.Group.LastSequence); err != nil {
		t.Fatal(err)
	}
	if _, err := sequencer.CommitWriteGroup(
		context.Background(), acquired.Group.ID, "commit-owner", testCommittedAt,
	); err == nil {
		t.Fatal("CommitWriteGroup succeeded with a poisoned member")
	}
	var committedMembers, retainedOutboxes int
	if err := db.SQLDB().QueryRowContext(t.Context(), `
		SELECT count(*) FILTER (WHERE state = 'committed'),
		       count(*) FILTER (WHERE length(outbox) > 0)
		FROM ingest_visibility_reservations`).Scan(
		&committedMembers,
		&retainedOutboxes,
	); err != nil {
		t.Fatal(err)
	}
	if committedMembers != 0 || retainedOutboxes != 2 {
		t.Fatalf("after rolled-back commit: committed=%d retained=%d", committedMembers, retainedOutboxes)
	}
	assertCutoff(t, sequencer, 0)
}

func TestSQLiteWriteGroupCommitClampsBackwardClockToSendingTime(t *testing.T) {
	t.Parallel()
	sequencer, db := openTestSequencer(t)
	createdAt := testCommittedAt.Add(-time.Hour)
	sequencer.now = func() time.Time { return createdAt }
	member := stageGroupMember(t, sequencer, "group-backward-clock", 1_000, 100)
	acquired, err := sequencer.FormOrAcquireWriteGroup(
		context.Background(), "backward-clock-owner", testWriteGroupLimits(), false,
	)
	if err != nil {
		t.Fatal(err)
	}
	sendingAt := testCommittedAt
	sequencer.now = func() time.Time { return sendingAt }
	if err := sequencer.MarkWriteGroupSending(
		context.Background(), acquired.Group.ID, "backward-clock-owner",
	); err != nil {
		t.Fatal(err)
	}
	committed, err := sequencer.CommitWriteGroup(
		context.Background(),
		acquired.Group.ID,
		"backward-clock-owner",
		sendingAt.Add(-time.Second),
	)
	if err != nil || !slices.Equal(committed, []uint64{member.Sequence}) {
		t.Fatalf("CommitWriteGroup = %v, %v", committed, err)
	}

	var groupCommittedAt, memberCommittedAt int64
	if err := db.SQLDB().QueryRowContext(t.Context(), `
		SELECT committed_at_unix_micro FROM ingest_write_groups WHERE write_group_id = ?`,
		acquired.Group.ID,
	).Scan(&groupCommittedAt); err != nil {
		t.Fatal(err)
	}
	if err := db.SQLDB().QueryRowContext(t.Context(), `
		SELECT committed_at_unix_micro
		FROM ingest_visibility_reservations WHERE sequence = ?`,
		member.Sequence,
	).Scan(&memberCommittedAt); err != nil {
		t.Fatal(err)
	}
	if groupCommittedAt != sendingAt.UnixMicro() || memberCommittedAt != sendingAt.UnixMicro() {
		t.Fatalf(
			"clamped commit timestamps = group %d member %d, want %d",
			groupCommittedAt,
			memberCommittedAt,
			sendingAt.UnixMicro(),
		)
	}
	terminal, found, err := sequencer.Lookup(
		context.Background(), member.BatchKey, member.SequenceKey, member.PayloadSHA256,
	)
	if err != nil || !found || !terminal.CommittedAt.Equal(sendingAt) {
		t.Fatalf("terminal Lookup = %+v found=%v error=%v", terminal, found, err)
	}
}

func TestSQLiteWriteGroupDigestFailsClosedOnAnyReplayMutation(t *testing.T) {
	t.Parallel()
	sequencer, _ := openTestSequencer(t)
	stageGroupMember(t, sequencer, "group-digest", 1_000, 100)
	acquired, err := sequencer.FormOrAcquireWriteGroup(
		context.Background(), "digest-owner", testWriteGroupLimits(), false,
	)
	if err != nil {
		t.Fatal(err)
	}
	mutations := map[string]func(*WriteGroup){
		"batch key": func(group *WriteGroup) { group.Members[0].BatchKey += "-changed" },
		"outbox":    func(group *WriteGroup) { group.Members[0].Outbox[0] ^= 0xff },
		"rows":      func(group *WriteGroup) { group.Members[0].StoredRowCount-- },
		"total":     func(group *WriteGroup) { group.RowCount++ },
		"digest":    func(group *WriteGroup) { group.MembershipSHA256[0] ^= 0xff },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			group := acquired.Group
			group.Members = append([]Reservation(nil), acquired.Group.Members...)
			group.Members[0].Outbox = slices.Clone(acquired.Group.Members[0].Outbox)
			mutate(&group)
			if err := ValidateWriteGroup(group, testWriteGroupLimits()); err == nil {
				t.Fatalf("ValidateWriteGroup accepted %s mutation", name)
			}
		})
	}
}

func TestSQLiteWriteGroupCommitTransitionsAllHECAcknowledgments(t *testing.T) {
	t.Parallel()
	sequencer, db := openTestSequencer(t)
	sequencer.now = func() time.Time { return testCommittedAt.Add(-time.Hour) }
	sequencer.hecAcknowledgmentIDs = &scriptedHECAcknowledgmentIDSource{ids: []uint64{701, 702}}
	insertHECTestToken(t, db.SQLDB(), "group-hec-token", 1, true)
	for index, key := range []string{"group-hec-one", "group-hec-two"} {
		request := reserveRequest(key, "stage-"+key)
		request.StoredRowCount = 500
		request.HECAdmission = &HECAdmissionRequest{
			TenantID:              "tenant-a",
			TokenID:               "group-hec-token",
			TokenVersion:          1,
			AuthorizedIndexes:     []HECIndexAuthority{{Name: "main", Version: 1}},
			RequestID:             "request-" + key,
			Acknowledgment:        true,
			AcknowledgmentChannel: "group-channel",
			CreatedAt:             request.IndexTime.Add(time.Duration(index) * time.Microsecond),
		}
		reservation, err := sequencer.Reserve(context.Background(), request)
		if err != nil {
			t.Fatal(err)
		}
		if err := sequencer.Release(
			context.Background(), reservation.Sequence, request.AttemptID,
		); err != nil {
			t.Fatal(err)
		}
	}
	acquired, err := sequencer.FormOrAcquireWriteGroup(
		context.Background(), "hec-group-owner", testWriteGroupLimits(), false,
	)
	if err != nil {
		t.Fatal(err)
	}
	sequencer.now = func() time.Time { return testCommittedAt.Add(-time.Second) }
	if err := sequencer.MarkWriteGroupSending(
		context.Background(), acquired.Group.ID, "hec-group-owner",
	); err != nil {
		t.Fatal(err)
	}
	if _, err := sequencer.CommitWriteGroup(
		context.Background(), acquired.Group.ID, "hec-group-owner", testCommittedAt,
	); err != nil {
		t.Fatal(err)
	}
	statuses, err := sequencer.LookupHECAcknowledgments(
		context.Background(),
		"tenant-a",
		"group-hec-token",
		"group-channel",
		[]uint64{701, 702},
	)
	if err != nil || !statuses[701] || !statuses[702] {
		t.Fatalf("group HEC statuses=%v error=%v", statuses, err)
	}
	var indexed, distinctTerminalTimes int
	if err := db.SQLDB().QueryRowContext(t.Context(), `
		SELECT count(*) FILTER (WHERE state = 'indexed'),
		       count(DISTINCT terminal_at_unix_micro)
		FROM hec_requests`).Scan(&indexed, &distinctTerminalTimes); err != nil {
		t.Fatal(err)
	}
	if indexed != 2 || distinctTerminalTimes != 1 {
		t.Fatalf("HEC indexed=%d terminal-times=%d", indexed, distinctTerminalTimes)
	}
}

func TestSQLiteWriteGroupPruneIsReferentiallySafe(t *testing.T) {
	t.Parallel()
	sequencer, db := openTestSequencer(t)
	sequencer.now = func() time.Time { return testCommittedAt.Add(-time.Hour) }
	stageGroupMember(t, sequencer, "group-prune-pending", 1_000, 100)
	acquired, err := sequencer.FormOrAcquireWriteGroup(
		context.Background(), "prune-owner", testWriteGroupLimits(), false,
	)
	if err != nil {
		t.Fatal(err)
	}
	if deleted, err := sequencer.PruneTerminal(
		context.Background(), TerminalRetention{}, 10,
	); err != nil || deleted != 0 {
		t.Fatalf("pending PruneTerminal=%d error=%v", deleted, err)
	}
	sequencer.now = func() time.Time { return testCommittedAt.Add(-time.Second) }
	if err := sequencer.MarkWriteGroupSending(
		context.Background(), acquired.Group.ID, "prune-owner",
	); err != nil {
		t.Fatal(err)
	}
	if _, err := sequencer.CommitWriteGroup(
		context.Background(), acquired.Group.ID, "prune-owner", testCommittedAt,
	); err != nil {
		t.Fatal(err)
	}
	if deleted, err := sequencer.PruneTerminal(
		context.Background(), TerminalRetention{}, 10,
	); err != nil || deleted != 1 {
		t.Fatalf("terminal PruneTerminal=%d error=%v", deleted, err)
	}
	for _, table := range []string{
		"ingest_write_group_members",
		"ingest_write_groups",
		"ingest_visibility_reservations",
	} {
		var count int
		if err := db.SQLDB().QueryRowContext(
			t.Context(),
			"SELECT count(*) FROM "+table,
		).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 0 {
			t.Fatalf("%s retained %d rows", table, count)
		}
	}
}

func TestSQLiteWriteGroupRejectsInvalidAndClosedOperations(t *testing.T) {
	t.Parallel()
	sequencer, _ := openTestSequencer(t)
	invalidLimits := testWriteGroupLimits()
	invalidLimits.MaxRows = 999
	if _, err := sequencer.FormOrAcquireWriteGroup(
		context.Background(), "invalid-owner", invalidLimits, false,
	); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("invalid limits error=%v", err)
	}
	if err := sequencer.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := sequencer.FormOrAcquireWriteGroup(
		context.Background(), "closed-owner", testWriteGroupLimits(), false,
	); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed FormOrAcquireWriteGroup error=%v", err)
	}
	if err := sequencer.MarkWriteGroupSending(
		context.Background(), "group", "closed-owner",
	); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed MarkWriteGroupSending error=%v", err)
	}
	if _, err := sequencer.CommitWriteGroup(
		context.Background(), "group", "closed-owner", testCommittedAt,
	); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed CommitWriteGroup error=%v", err)
	}
	if err := sequencer.ReleaseWriteGroup(
		context.Background(), "group", "closed-owner",
	); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed ReleaseWriteGroup error=%v", err)
	}
}

func TestWriteGroupMembershipSHA256IsOrderAndIdentitySensitive(t *testing.T) {
	t.Parallel()
	outboxOne := []byte("one")
	outboxTwo := []byte("two")
	members := []Reservation{
		{
			BatchKey:          "one",
			Sequence:          1,
			PayloadSHA256:     sha256.Sum256([]byte("payload-one")),
			Outbox:            outboxOne,
			OutboxSHA256:      sha256.Sum256(outboxOne),
			StoredRowCount:    1,
			DecodedEventBytes: 3,
		},
		{
			BatchKey:          "two",
			Sequence:          2,
			PayloadSHA256:     sha256.Sum256([]byte("payload-two")),
			Outbox:            outboxTwo,
			OutboxSHA256:      sha256.Sum256(outboxTwo),
			StoredRowCount:    1,
			DecodedEventBytes: 3,
		},
	}
	first, err := WriteGroupMembershipSHA256(members)
	if err != nil {
		t.Fatal(err)
	}
	members[1].BatchKey = "three"
	second, err := WriteGroupMembershipSHA256(members)
	if err != nil {
		t.Fatal(err)
	}
	if first == second {
		t.Fatal("membership digest ignored logical identity")
	}
	slices.Reverse(members)
	if _, err := WriteGroupMembershipSHA256(members); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("reordered membership error=%v", err)
	}
}
