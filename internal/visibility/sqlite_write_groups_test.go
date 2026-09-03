package visibility

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"testing"
	"time"
)

func TestSQLiteWriteGroupLingerFormationAndAtomicCommit(t *testing.T) {
	t.Parallel()
	sequencer, db := openTestSequencer(t)
	ctx := context.Background()
	createdAt := time.Date(2026, time.August, 1, 2, 3, 4, 500000000, time.UTC)
	sequencer.now = func() time.Time { return createdAt }
	first := stageWriteGroupMember(t, sequencer, "group-first", 3, 30)
	second := stageWriteGroupMember(t, sequencer, "group-second", 4, 40)
	limits := testWriteGroupLimits()

	group, found, deadline, err := sequencer.FormOrAcquireWriteGroup(
		ctx,
		"group-owner",
		limits,
		createdAt.Add(limits.MaxLinger-time.Microsecond),
	)
	if err != nil || found || !deadline.Equal(createdAt.Add(limits.MaxLinger)) {
		t.Fatalf("pre-linger formation = group %+v found=%v deadline=%v error=%v", group, found, deadline, err)
	}

	group, found, deadline, err = sequencer.FormOrAcquireWriteGroup(
		ctx,
		"group-owner",
		limits,
		createdAt.Add(limits.MaxLinger),
	)
	if err != nil || !found || !deadline.IsZero() {
		t.Fatalf("linger formation = group %+v found=%v deadline=%v error=%v", group, found, deadline, err)
	}
	if group.State != WriteGroupReady || group.AttemptID != "group-owner" ||
		len(group.Members) != 2 || group.RowCount != 7 || group.DecodedBytes != 70 ||
		group.FirstSequence != first.Sequence || group.LastSequence != second.Sequence ||
		group.Members[0].Reservation.Sequence != first.Sequence ||
		group.Members[1].Reservation.Sequence != second.Sequence {
		t.Fatalf("formed write group = %+v", group)
	}
	computed, err := ComputeWriteGroupMembershipSHA256(group.Members)
	if err != nil || computed != group.MembershipSHA256 {
		t.Fatalf("membership digest = %x error=%v, want %x", computed, err, group.MembershipSHA256)
	}
	usage, err := sequencer.PendingUsage(ctx)
	if err != nil {
		t.Fatal(err)
	}
	wantMetadataBytes := uint64(len(first.Metadata) + len(second.Metadata))
	if usage.Reservations != 2 || usage.UngroupedReservations != 0 ||
		usage.ReadyGroups != 1 || usage.AmbiguousGroups != 0 || usage.LiveGroupLeases != 1 ||
		usage.MetadataBytes != wantMetadataBytes || !usage.OldestPendingAt.Equal(createdAt) {
		t.Fatalf("formed pending usage = %+v, want ready group metadata=%d", usage, wantMetadataBytes)
	}

	sequencer.now = func() time.Time { return createdAt.Add(time.Second) }
	if err := sequencer.MarkWriteGroupSending(ctx, group.ID, "group-owner"); err != nil {
		t.Fatal(err)
	}
	assertWriteGroupMemberPhases(t, db, group.ID, phaseAmbiguous, reservationReserved, true)
	usage, err = sequencer.PendingUsage(ctx)
	if err != nil || usage.ReadyGroups != 0 || usage.AmbiguousGroups != 1 || usage.LiveGroupLeases != 1 {
		t.Fatalf("ambiguous pending usage = %+v error=%v", usage, err)
	}

	committedAt := createdAt.Add(2 * time.Second)
	if err := sequencer.CommitWriteGroup(ctx, group.ID, "group-owner", committedAt); err != nil {
		t.Fatal(err)
	}
	assertWriteGroupMemberPhases(t, db, group.ID, phaseFinal, reservationCommitted, false)
	assertCutoff(t, sequencer, second.Sequence)
	var storedState, attemptID string
	var storedCommittedAt int64
	if err := db.SQLDB().QueryRowContext(ctx, `
		SELECT state, attempt_id, committed_at_unix_micro
		FROM ingest_write_groups WHERE write_group_id = ?`, group.ID).Scan(
		&storedState,
		&attemptID,
		&storedCommittedAt,
	); err != nil {
		t.Fatal(err)
	}
	if storedState != string(WriteGroupCommitted) || attemptID != "" ||
		storedCommittedAt != committedAt.UnixMicro() {
		t.Fatalf("terminal group = state %q attempt %q committed %d", storedState, attemptID, storedCommittedAt)
	}
}

func TestSQLiteWriteGroupHardBoundaryAndDurableRestartRecovery(t *testing.T) {
	t.Parallel()
	sequencer, db := openTestSequencer(t)
	ctx := context.Background()
	now := time.Date(2026, time.August, 2, 3, 4, 5, 0, time.UTC)
	sequencer.now = func() time.Time { return now }
	first := stageWriteGroupMember(t, sequencer, "hard-first", 3, 30)
	_ = stageWriteGroupMember(t, sequencer, "hard-second", 3, 30)
	limits := testWriteGroupLimits()
	limits.TargetRows = 5
	limits.HardMaxRows = 5

	group, found, _, err := sequencer.FormOrAcquireWriteGroup(ctx, "first-process", limits, now)
	if err != nil || !found {
		t.Fatalf("hard-bound formation found=%v error=%v", found, err)
	}
	if len(group.Members) != 1 || group.RowCount != 3 ||
		group.Members[0].Reservation.Sequence != first.Sequence {
		t.Fatalf("hard-bound group = %+v", group)
	}
	if err := sequencer.MarkWriteGroupSending(ctx, group.ID, "first-process"); err != nil {
		t.Fatal(err)
	}
	wantDigest := group.MembershipSHA256
	if err := sequencer.Close(); err != nil {
		t.Fatal(err)
	}
	sequencer = waitForReplacementVisibilityOwner(t, db)

	replayed, found, _, err := sequencer.FormOrAcquireWriteGroup(ctx, "second-process", limits, now.Add(time.Second))
	if err != nil || !found {
		t.Fatalf("restart acquisition found=%v error=%v", found, err)
	}
	if replayed.ID != group.ID || replayed.State != WriteGroupAmbiguous ||
		replayed.MembershipSHA256 != wantDigest || len(replayed.Members) != 1 ||
		replayed.Members[0].Reservation.Sequence != first.Sequence {
		t.Fatalf("replayed group = %+v, want stable %+v", replayed, group)
	}
	if err := sequencer.ReleaseWriteGroup(ctx, replayed.ID, "second-process"); err != nil {
		t.Fatal(err)
	}
}

func TestSQLiteWriteGroupAmbiguousBarrierStillAllowsAdmission(t *testing.T) {
	t.Parallel()
	sequencer, _ := openTestSequencer(t)
	ctx := context.Background()
	now := time.Date(2026, time.August, 3, 4, 5, 6, 0, time.UTC)
	sequencer.now = func() time.Time { return now }
	first := stageWriteGroupMember(t, sequencer, "barrier-first", 1, 10)
	limits := testWriteGroupLimits()
	limits.ForceSeal = true
	group, found, _, err := sequencer.FormOrAcquireWriteGroup(ctx, "barrier-owner", limits, now)
	if err != nil || !found {
		t.Fatalf("form ambiguous barrier found=%v error=%v", found, err)
	}
	if err := sequencer.MarkWriteGroupSending(ctx, group.ID, "barrier-owner"); err != nil {
		t.Fatal(err)
	}
	if err := sequencer.ReleaseWriteGroup(ctx, group.ID, "barrier-owner"); err != nil {
		t.Fatal(err)
	}

	second := stageWriteGroupMember(t, sequencer, "barrier-second", 1, 10)
	if second.Sequence <= first.Sequence {
		t.Fatalf("new admission sequence = %d, want after %d", second.Sequence, first.Sequence)
	}
	recovered, found, _, err := sequencer.FormOrAcquireWriteGroup(ctx, "recovery-owner", limits, now.Add(time.Second))
	if err != nil || !found || recovered.ID != group.ID || recovered.State != WriteGroupAmbiguous {
		t.Fatalf("barrier acquisition = %+v found=%v error=%v", recovered, found, err)
	}
	if len(recovered.Members) != 1 || recovered.Members[0].Reservation.Sequence != first.Sequence {
		t.Fatalf("ambiguous barrier members = %+v", recovered.Members)
	}
}

func TestSQLiteWriteGroupCompetingAttemptsCannotOverlap(t *testing.T) {
	t.Parallel()
	sequencer, _ := openTestSequencer(t)
	ctx := context.Background()
	now := time.Date(2026, time.August, 3, 5, 6, 7, 0, time.UTC)
	sequencer.now = func() time.Time { return now }
	_ = stageWriteGroupMember(t, sequencer, "lease-member", 1, 10)
	limits := testWriteGroupLimits()
	limits.ForceSeal = true
	owned, found, _, err := sequencer.FormOrAcquireWriteGroup(ctx, "lease-owner-one", limits, now)
	if err != nil || !found {
		t.Fatalf("first acquisition found=%v error=%v", found, err)
	}
	if competing, competingFound, deadline, err := sequencer.FormOrAcquireWriteGroup(
		ctx,
		"lease-owner-two",
		limits,
		now,
	); err != nil || competingFound || !deadline.IsZero() || competing.ID != "" {
		t.Fatalf("competing acquisition = %+v found=%v deadline=%v error=%v", competing, competingFound, deadline, err)
	}
	if err := sequencer.ReleaseWriteGroup(ctx, owned.ID, "lease-owner-one"); err != nil {
		t.Fatal(err)
	}
	reacquired, found, _, err := sequencer.FormOrAcquireWriteGroup(ctx, "lease-owner-two", limits, now)
	if err != nil || !found || reacquired.ID != owned.ID {
		t.Fatalf("reacquisition = %+v found=%v error=%v", reacquired, found, err)
	}
}

func TestSQLiteWriteGroupReclaimsInactiveReservationOwnerWithoutRestart(t *testing.T) {
	t.Parallel()
	sequencer, db := openTestSequencer(t)
	ctx := context.Background()
	now := time.Date(2026, time.August, 3, 6, 7, 8, 0, time.UTC)
	sequencer.now = func() time.Time { return now }
	request := reserveRequest("stale-request-owner", "stale-request-attempt")
	request.StoredRowCount = 1
	request.DecodedEventBytes = 10
	reservation, err := sequencer.Reserve(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	// Model an outcome-ambiguous Release: the process-local lease is gone but
	// SQLite may still contain the old owner because the caller did not observe
	// the transaction result. Reconciliation must recover without a restart.
	sequencer.leases.deactivate(request.AttemptID)

	limits := testWriteGroupLimits()
	limits.ForceSeal = true
	group, found, _, err := sequencer.FormOrAcquireWriteGroup(ctx, "group-recovery", limits, now)
	if err != nil || !found {
		t.Fatalf("FormOrAcquireWriteGroup found=%v error=%v", found, err)
	}
	if len(group.Members) != 1 || group.Members[0].Reservation.Sequence != reservation.Sequence {
		t.Fatalf("recovered group = %+v, want reservation %d", group, reservation.Sequence)
	}
	var durableOwner string
	if err := db.SQLDB().QueryRowContext(ctx, `
		SELECT attempt_id
		FROM ingest_visibility_reservations
		WHERE sequence = ?`, reservation.Sequence).Scan(&durableOwner); err != nil {
		t.Fatal(err)
	}
	if durableOwner != "" {
		t.Fatalf("recovered reservation owner = %q, want empty group-owned lease", durableOwner)
	}
}

func TestSQLiteWriteGroupCommitRollsBackEveryMember(t *testing.T) {
	t.Parallel()
	sequencer, db := openTestSequencer(t)
	ctx := context.Background()
	now := time.Date(2026, time.August, 4, 5, 6, 7, 0, time.UTC)
	sequencer.now = func() time.Time { return now }
	_ = stageWriteGroupMember(t, sequencer, "rollback-first", 1, 10)
	second := stageWriteGroupMember(t, sequencer, "rollback-second", 1, 10)
	limits := testWriteGroupLimits()
	limits.ForceSeal = true
	group, found, _, err := sequencer.FormOrAcquireWriteGroup(ctx, "rollback-owner", limits, now)
	if err != nil || !found {
		t.Fatalf("form rollback group found=%v error=%v", found, err)
	}
	if err := sequencer.MarkWriteGroupSending(ctx, group.ID, "rollback-owner"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.SQLDB().ExecContext(ctx, `
		CREATE TRIGGER test_fail_second_group_commit
		BEFORE UPDATE OF state ON ingest_visibility_reservations
		WHEN NEW.state = 'committed' AND NEW.sequence = `+writeTestUint(second.Sequence)+`
		BEGIN
			SELECT RAISE(ABORT, 'injected group commit failure');
		END`); err != nil {
		t.Fatal(err)
	}
	if err := sequencer.CommitWriteGroup(ctx, group.ID, "rollback-owner", now.Add(time.Second)); err == nil {
		t.Fatal("CommitWriteGroup() succeeded through injected member failure")
	}
	assertWriteGroupMemberPhases(t, db, group.ID, phaseAmbiguous, reservationReserved, true)
	assertCutoff(t, sequencer, 0)
	if _, err := db.SQLDB().ExecContext(ctx, `DROP TRIGGER test_fail_second_group_commit`); err != nil {
		t.Fatal(err)
	}
	recovered, found, _, err := sequencer.FormOrAcquireWriteGroup(ctx, "rollback-recovery", limits, now.Add(2*time.Second))
	if err != nil || !found || recovered.ID != group.ID || recovered.State != WriteGroupAmbiguous {
		t.Fatalf("reacquire rollback group = %+v found=%v error=%v", recovered, found, err)
	}
	if err := sequencer.CommitWriteGroup(ctx, group.ID, "rollback-recovery", now.Add(2*time.Second)); err != nil {
		t.Fatalf("retry CommitWriteGroup(): %v", err)
	}
}

func TestSQLiteWriteGroupOutboxCorruptionFailsClosed(t *testing.T) {
	t.Parallel()
	sequencer, db := openTestSequencer(t)
	ctx := context.Background()
	now := time.Date(2026, time.August, 5, 6, 7, 8, 0, time.UTC)
	sequencer.now = func() time.Time { return now }
	reservation := stageWriteGroupMember(t, sequencer, "corrupt-outbox", 1, 10)
	limits := testWriteGroupLimits()
	limits.ForceSeal = true
	group, found, _, err := sequencer.FormOrAcquireWriteGroup(ctx, "corrupt-owner", limits, now)
	if err != nil || !found {
		t.Fatalf("form corruption group found=%v error=%v", found, err)
	}
	if err := sequencer.ReleaseWriteGroup(ctx, group.ID, "corrupt-owner"); err != nil {
		t.Fatal(err)
	}
	corrupted := make([]byte, len(reservation.Outbox))
	copy(corrupted, reservation.Outbox)
	corrupted[0] ^= 0xff
	if _, err := db.SQLDB().ExecContext(ctx, `
		UPDATE ingest_visibility_reservations SET outbox = ? WHERE sequence = ?`,
		corrupted,
		reservation.Sequence,
	); err != nil {
		t.Fatal(err)
	}
	if _, found, _, err := sequencer.FormOrAcquireWriteGroup(ctx, "corrupt-replay", limits, now); err == nil || found {
		t.Fatalf("corrupted group acquisition found=%v error=%v, want fail closed", found, err)
	}
}

func TestSQLiteWriteGroupSchemaRejectsInvalidMembershipAndTransitions(t *testing.T) {
	t.Parallel()
	sequencer, db := openTestSequencer(t)
	ctx := context.Background()
	now := time.Date(2026, time.August, 6, 7, 8, 9, 0, time.UTC)
	sequencer.now = func() time.Time { return now }
	reservation := stageWriteGroupMember(t, sequencer, "schema-group", 1, 10)
	limits := testWriteGroupLimits()
	limits.ForceSeal = true
	group, found, _, err := sequencer.FormOrAcquireWriteGroup(ctx, "schema-owner", limits, now)
	if err != nil || !found {
		t.Fatalf("form schema group found=%v error=%v", found, err)
	}
	if _, err := db.SQLDB().ExecContext(ctx, `
		UPDATE ingest_write_groups
		SET state = 'committed', sending_at_unix_micro = ?, committed_at_unix_micro = ?, attempt_id = ''
		WHERE write_group_id = ?`, now.UnixMicro(), now.UnixMicro(), group.ID); err == nil {
		t.Fatal("schema accepted ready-to-committed transition")
	}
	duplicateDigest := sha256.Sum256([]byte("duplicate"))
	if _, err := db.SQLDB().ExecContext(ctx, `
		INSERT INTO ingest_write_group_members
			(write_group_id, ordinal, visibility_sequence, row_count, decoded_bytes, outbox_sha256)
		VALUES (?, 2, ?, 1, 1, ?)`,
		group.ID,
		reservation.Sequence,
		duplicateDigest[:],
	); err == nil {
		t.Fatal("schema accepted duplicate/gapped write group membership")
	}
	for _, invalid := range []struct {
		name        string
		state       string
		createdAt   int64
		sendingAt   any
		committedAt any
	}{
		{name: "zero creation", state: "ready", createdAt: 0},
		{name: "creation outside range", state: "ready", createdAt: 253402300800000000},
		{name: "sending before creation", state: "ambiguous", createdAt: now.UnixMicro(), sendingAt: now.Add(-time.Microsecond).UnixMicro()},
		{name: "sending outside range", state: "ambiguous", createdAt: now.UnixMicro(), sendingAt: int64(253402300800000000)},
		{name: "commit outside range", state: "committed", createdAt: now.UnixMicro(), sendingAt: now.UnixMicro(), committedAt: int64(253402300800000000)},
	} {
		t.Run(invalid.name, func(t *testing.T) {
			if _, err := db.SQLDB().ExecContext(ctx, `
				INSERT INTO ingest_write_groups
					(write_group_id, state, attempt_id, member_count, row_count,
					 decoded_bytes, membership_sha256, first_sequence, last_sequence,
					 created_at_unix_micro, sending_at_unix_micro, committed_at_unix_micro)
				VALUES (?, ?, '', 1, 1, 1, ?, 1, 1, ?, ?, ?)`,
				"invalid-time-"+invalid.name,
				invalid.state,
				duplicateDigest[:],
				invalid.createdAt,
				invalid.sendingAt,
				invalid.committedAt,
			); err == nil {
				t.Fatalf("schema accepted invalid %s timestamp", invalid.name)
			}
		})
	}
}

func TestSQLiteWriteGroupPruneRemovesMembershipBeforeLogicalAuthority(t *testing.T) {
	t.Parallel()
	sequencer, db := openTestSequencer(t)
	ctx := context.Background()
	now := time.Date(2026, time.August, 7, 8, 9, 10, 0, time.UTC)
	sequencer.now = func() time.Time { return now }
	_ = stageWriteGroupMember(t, sequencer, "pruned-group", 1, 10)
	limits := testWriteGroupLimits()
	limits.ForceSeal = true
	group, found, _, err := sequencer.FormOrAcquireWriteGroup(ctx, "prune-owner", limits, now)
	if err != nil || !found {
		t.Fatalf("form prune group found=%v error=%v", found, err)
	}
	if err := sequencer.MarkWriteGroupSending(ctx, group.ID, "prune-owner"); err != nil {
		t.Fatal(err)
	}
	if err := sequencer.CommitWriteGroup(ctx, group.ID, "prune-owner", now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	deleted, err := sequencer.PruneTerminal(ctx, TerminalRetention{}, 10)
	if err != nil || deleted != 1 {
		t.Fatalf("PruneTerminal() deleted=%d error=%v", deleted, err)
	}
	for _, table := range []string{
		"ingest_write_group_members",
		"ingest_write_groups",
		"ingest_visibility_reservations",
	} {
		var count int
		if err := db.SQLDB().QueryRowContext(ctx, "SELECT count(*) FROM "+table).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 0 {
			t.Fatalf("%s rows after prune = %d, want 0", table, count)
		}
	}
}

func TestValidateBackupDrainRejectsEveryPendingWriteShape(t *testing.T) {
	t.Parallel()
	for _, state := range []string{"ungrouped", "ready", "ambiguous", "leased"} {
		t.Run(state, func(t *testing.T) {
			t.Parallel()
			sequencer, database := openTestSequencer(t)
			ctx := context.Background()
			now := time.Date(2026, time.August, 8, 9, 10, 11, 0, time.UTC)
			sequencer.now = func() time.Time { return now }
			if err := ValidateBackupDrain(ctx, database); err != nil {
				t.Fatalf("empty backup drain: %v", err)
			}
			_ = stageWriteGroupMember(t, sequencer, "backup-"+state, 1, 10)
			if state != "ungrouped" {
				limits := testWriteGroupLimits()
				limits.ForceSeal = true
				group, found, _, err := sequencer.FormOrAcquireWriteGroup(ctx, "backup-owner", limits, now)
				if err != nil || !found {
					t.Fatalf("form backup group found=%v error=%v", found, err)
				}
				switch state {
				case "ready":
					if err := sequencer.ReleaseWriteGroup(ctx, group.ID, "backup-owner"); err != nil {
						t.Fatal(err)
					}
				case "ambiguous":
					if err := sequencer.MarkWriteGroupSending(ctx, group.ID, "backup-owner"); err != nil {
						t.Fatal(err)
					}
					if err := sequencer.ReleaseWriteGroup(ctx, group.ID, "backup-owner"); err != nil {
						t.Fatal(err)
					}
				}
			}
			if err := ValidateBackupDrain(ctx, database); err == nil {
				t.Fatalf("backup accepted %s pending state", state)
			}
		})
	}
}

func testWriteGroupLimits() WriteGroupLimits {
	return WriteGroupLimits{
		TargetRows:          10,
		HardMaxRows:         50,
		TargetDecodedBytes:  100,
		HardMaxDecodedBytes: 500,
		MaxMembers:          10,
		MaxLinger:           200 * time.Millisecond,
	}
}

func stageWriteGroupMember(
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

func assertWriteGroupMemberPhases(
	t *testing.T,
	db interface{ SQLDB() *sql.DB },
	groupID string,
	wantPhase string,
	wantState string,
	wantOutbox bool,
) {
	t.Helper()
	rows, err := db.SQLDB().QueryContext(context.Background(), `
		SELECT reservation.state, reservation.phase, length(reservation.outbox),
		       reservation.committed_at_unix_micro
		FROM ingest_write_group_members AS member
		JOIN ingest_visibility_reservations AS reservation
		  ON reservation.sequence = member.visibility_sequence
		WHERE member.write_group_id = ?
		ORDER BY member.ordinal`, groupID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		var state, phase string
		var outboxBytes int
		var committedAt any
		if err := rows.Scan(&state, &phase, &outboxBytes, &committedAt); err != nil {
			t.Fatal(err)
		}
		if state != wantState || phase != wantPhase || (outboxBytes > 0) != wantOutbox {
			t.Fatalf("member %d = state %q phase %q outbox %d", count, state, phase, outboxBytes)
		}
		count++
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if count == 0 {
		t.Fatal("write group has no persisted members")
	}
}

func writeTestUint(value uint64) string {
	const digits = "0123456789"
	if value == 0 {
		return "0"
	}
	var encoded [20]byte
	index := len(encoded)
	for value != 0 {
		index--
		encoded[index] = digits[value%10]
		value /= 10
	}
	return string(encoded[index:])
}
