package visibility

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"sync"
	"testing"
	"testing/fstest"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/ingestquota"
	"github.com/Suhaibinator/open-splunk/migrations"
)

func TestSQLitePrincipalCapacityLeavesRoomAndRollsBackQuota(t *testing.T) {
	sequencer, db := openTestSequencer(t)
	ctx := context.Background()
	principal := sha256.Sum256([]byte("source-a"))
	seedPrincipalPending(t, db, principal[:], MaxPrincipalPendingReservations-1, 1, 0)
	token := ingestquota.ScopeKey{Kind: ingestquota.ScopeKindToken, TenantID: "tenant-a", Identity: "token-a"}
	seedQuotaToken(t, db, token.Identity)
	request := quotaReserveRequest("last-principal-slot", "last-owner", testCommittedAt, token, ingestquota.Limits{}, ingestquota.Limits{})
	request.PrincipalSHA256 = principal
	if _, err := sequencer.Reserve(ctx, request); err != nil {
		t.Fatalf("last principal slot: %v", err)
	}
	request.BatchKey = "blocked-principal-slot"
	request.SequenceKey = "sequence-blocked-principal-slot"
	request.AttemptID = "blocked-owner"
	request.QuotaAdmission = quotaAdmission(token, ingestquota.Limits{MaxEventsPerSecond: 1}, "main", ingestquota.Limits{MaxEventsPerSecond: 1})
	if _, err := sequencer.Reserve(ctx, request); !errors.Is(err, ErrPendingCapacity) {
		t.Fatalf("principal at capacity: %v", err)
	}
	assertQuotaObjectCounts(t, db, MaxPrincipalPendingReservations, MaxPrincipalPendingReservations, 1)
	if buckets := readQuotaBuckets(t, db); len(buckets) != 0 {
		t.Fatalf("capacity denial charged rate quotas: %+v", buckets)
	}
	other := reserveRequest("other-principal", "other-owner")
	other.PrincipalSHA256 = sha256.Sum256([]byte("source-b"))
	accepted, err := sequencer.Reserve(ctx, other)
	if err != nil || accepted.Sequence != MaxPrincipalPendingReservations+1 {
		t.Fatalf("independent principal admission = %+v, %v", accepted, err)
	}
	ready, err := sequencer.HECReadiness(ctx)
	if err != nil || !ready.QueueAvailable {
		t.Fatalf("one saturated principal changed global health: %+v, %v", ready, err)
	}
}

func TestSQLitePrincipalByteCapacityLeavesRoom(t *testing.T) {
	// Exercise real stored lengths at each byte ceiling. Run the fixtures
	// sequentially so this boundary check holds only one large database at once.
	for _, test := range []struct {
		name          string
		count         int
		outboxBytes   int
		metadataBytes int
	}{
		{name: "outbox", count: MaxPrincipalPendingOutboxBytes / MaxOutboxBytes, outboxBytes: MaxOutboxBytes},
		{name: "metadata", count: MaxPrincipalPendingMetadataBytes / MaxMetadataBytes, outboxBytes: 1, metadataBytes: MaxMetadataBytes},
	} {
		t.Run(test.name, func(t *testing.T) {
			sequencer, db := openTestSequencer(t)
			principal := sha256.Sum256([]byte("large-source"))
			seedPrincipalPending(t, db, principal[:], test.count, test.outboxBytes, test.metadataBytes)
			request := reserveRequest("large-source-next", "large-owner")
			request.PrincipalSHA256 = principal
			if _, err := sequencer.Reserve(context.Background(), request); !errors.Is(err, ErrPendingCapacity) {
				t.Fatalf("principal byte ceiling: %v", err)
			}
			request.PrincipalSHA256 = sha256.Sum256([]byte("small-source"))
			if _, err := sequencer.Reserve(context.Background(), request); err != nil {
				t.Fatalf("other principal lost byte headroom: %v", err)
			}
		})
	}
}

func TestPrincipalPendingCapacityBoundaries(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name                       string
		count, outbox, extraOutbox int64
		metadata, extraMetadata    int64
		want                       bool
	}{
		{name: "last row", count: MaxPrincipalPendingReservations - 1},
		{name: "full rows", count: MaxPrincipalPendingReservations, want: true},
		{name: "outbox fits", outbox: MaxPrincipalPendingOutboxBytes - MaxOutboxBytes, extraOutbox: MaxOutboxBytes},
		{name: "outbox exceeds", outbox: MaxPrincipalPendingOutboxBytes - MaxOutboxBytes + 1, extraOutbox: MaxOutboxBytes, want: true},
		{name: "metadata fits", metadata: MaxPrincipalPendingMetadataBytes - MaxMetadataBytes, extraMetadata: MaxMetadataBytes},
		{name: "metadata exceeds", metadata: MaxPrincipalPendingMetadataBytes - MaxMetadataBytes + 1, extraMetadata: MaxMetadataBytes, want: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := principalPendingCapacityExceeded(test.count, test.outbox, test.extraOutbox, test.metadata, test.extraMetadata); got != test.want {
				t.Fatalf("capacity exceeded = %t, want %t", got, test.want)
			}
		})
	}
}

func TestSQLitePrincipalAccountingSurvivesReleaseRestartAndReplay(t *testing.T) {
	sequencer, db := openTestSequencer(t)
	ctx := context.Background()
	request := reserveRequest("principal-lifecycle", "initial-owner")
	seedPrincipalPending(t, db, request.PrincipalSHA256[:], MaxPrincipalPendingReservations-1, 1, 0)
	first, err := sequencer.Reserve(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if err := sequencer.Release(ctx, first.Sequence, request.AttemptID); err != nil {
		t.Fatal(err)
	}
	if _, err := sequencer.Reserve(ctx, reserveRequest("after-release", "blocked-owner")); !errors.Is(err, ErrPendingCapacity) {
		t.Fatalf("release freed capacity: %v", err)
	}
	if err := sequencer.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := NewSQLite(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	if _, err := reopened.Reserve(ctx, reserveRequest("after-restart", "blocked-owner")); !errors.Is(err, ErrPendingCapacity) {
		t.Fatalf("restart freed capacity: %v", err)
	}
	replay := ReserveRequest{
		BatchKey: request.BatchKey, SequenceKey: request.SequenceKey,
		AttemptID: "replay-owner", ExistingOnly: true, PayloadSHA256: request.PayloadSHA256,
	}
	resumed, err := reopened.Reserve(ctx, replay)
	if err != nil || !resumed.PreviouslyReserved || resumed.Sequence != first.Sequence {
		t.Fatalf("replay at capacity = %+v, %v", resumed, err)
	}
	markAndCommit(t, reopened, resumed.Sequence, replay.AttemptID, testCommittedAt)
	if _, err := reopened.Reserve(ctx, reserveRequest("after-commit", "new-owner")); err != nil {
		t.Fatalf("commit did not free capacity: %v", err)
	}
	terminal, err := reopened.Reserve(ctx, replay)
	if err != nil || !terminal.AlreadyCommitted {
		t.Fatalf("terminal replay at capacity = %+v, %v", terminal, err)
	}
}

func TestSQLitePrincipalAbandonedReplacementRechecksCapacityWithoutRecharging(t *testing.T) {
	sequencer, db := openTestSequencer(t)
	ctx := context.Background()
	token := ingestquota.ScopeKey{Kind: ingestquota.ScopeKindToken, TenantID: "tenant-a", Identity: "token-a"}
	seedQuotaToken(t, db, token.Identity)
	request := quotaReserveRequest("principal-replacement", "original-owner", testCommittedAt, token, ingestquota.Limits{MaxEventsPerSecond: 1}, ingestquota.Limits{})
	first, err := sequencer.Reserve(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if err := sequencer.Abandon(ctx, first.Sequence, request.AttemptID); err != nil {
		t.Fatal(err)
	}
	before := readQuotaBuckets(t, db)
	seedPrincipalPending(t, db, request.PrincipalSHA256[:], MaxPrincipalPendingReservations, 1, 0)
	request.AttemptID = "replacement-owner"
	if _, err := sequencer.Reserve(ctx, request); !errors.Is(err, ErrPendingCapacity) {
		t.Fatalf("durable rate marker bypassed pending capacity: %v", err)
	}
	oldest, found, err := sequencer.AcquirePending(ctx, "drain-owner")
	if err != nil || !found {
		t.Fatalf("acquire while full: found=%t, %v", found, err)
	}
	if err := sequencer.Abandon(ctx, oldest.Sequence, "drain-owner"); err != nil {
		t.Fatal(err)
	}
	if _, err := sequencer.Reserve(ctx, request); err != nil {
		t.Fatalf("replacement after drain: %v", err)
	}
	if after := readQuotaBuckets(t, db); !equalQuotaBuckets(before, after) {
		t.Fatalf("replacement recharged accepted work: before=%+v after=%+v", before, after)
	}
}

func TestSQLitePrincipalConcurrentAdmissionIsAtomic(t *testing.T) {
	sequencer, db := openTestSequencer(t)
	principal := sha256.Sum256([]byte("concurrent-source"))
	seedPrincipalPending(t, db, principal[:], MaxPrincipalPendingReservations-1, 1, 0)
	const contenders = 12
	start := make(chan struct{})
	results := make(chan error, contenders)
	var wait sync.WaitGroup
	for index := range contenders {
		wait.Go(func() {
			<-start
			request := reserveRequest(fmt.Sprintf("concurrent-%d", index), fmt.Sprintf("owner-%d", index))
			request.PrincipalSHA256 = principal
			ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
			defer cancel()
			for {
				_, err := sequencer.Reserve(ctx, request)
				if !control.IsDatabaseContention(err) {
					results <- err
					return
				}
				select {
				case <-ctx.Done():
					results <- errors.Join(ctx.Err(), err)
					return
				case <-time.After(time.Millisecond):
				}
			}
		})
	}
	close(start)
	wait.Wait()
	close(results)
	accepted := 0
	for err := range results {
		if err == nil {
			accepted++
		} else if !errors.Is(err, ErrPendingCapacity) {
			t.Fatalf("concurrent admission error: %v", err)
		}
	}
	if accepted != 1 {
		t.Fatalf("concurrent successful admissions = %d, want 1", accepted)
	}
	usage, err := sequencer.PendingUsage(context.Background())
	if err != nil || usage.Reservations != MaxPrincipalPendingReservations {
		t.Fatalf("concurrent pending usage = %+v, %v", usage, err)
	}
}

func TestSQLitePrincipalGroupRecoveryRetainsAndFreesCapacity(t *testing.T) {
	sequencer, db := openTestSequencer(t)
	ctx := context.Background()
	principal := sha256.Sum256([]byte("group-source"))
	seedPrincipalPending(t, db, principal[:], MaxPrincipalPendingReservations, 1, 0)
	limits := testWriteGroupLimits()
	group, found, _, err := sequencer.FormOrAcquireWriteGroup(ctx, "group-owner", limits, testCommittedAt)
	if err != nil || !found {
		t.Fatalf("form group at capacity: found=%t, %v", found, err)
	}
	request := reserveRequest("after-group", "next-owner")
	request.PrincipalSHA256 = principal
	if _, err := sequencer.Reserve(ctx, request); !errors.Is(err, ErrPendingCapacity) {
		t.Fatalf("group formation freed capacity: %v", err)
	}
	if err := sequencer.MarkWriteGroupSending(ctx, group.ID, "group-owner"); err != nil {
		t.Fatal(err)
	}
	if err := sequencer.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := NewSQLite(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	recovered, found, _, err := reopened.FormOrAcquireWriteGroup(ctx, "recovery-owner", limits, testCommittedAt.Add(time.Second))
	if err != nil || !found || recovered.ID != group.ID || recovered.State != WriteGroupAmbiguous {
		t.Fatalf("recover ambiguous group = %+v found=%t, %v", recovered, found, err)
	}
	if _, err := reopened.Reserve(ctx, request); !errors.Is(err, ErrPendingCapacity) {
		t.Fatalf("leased ambiguous group lost pending charge: %v", err)
	}
	if err := reopened.CommitWriteGroup(ctx, recovered.ID, "recovery-owner", testCommittedAt.Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := reopened.Reserve(ctx, request); err != nil {
		t.Fatalf("group commit did not free capacity: %v", err)
	}
}

func TestSQLitePrincipalUpgradePreservesUnattributedDebt(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "legacy.sqlite")
	raw, err := sql.Open("sqlite", path+"?_txlock=immediate&_pragma=foreign_keys(1)")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = raw.Close() })
	prefix := fstest.MapFS{}
	entries, err := fs.ReadDir(migrations.SQLite(), ".")
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.Name() >= "0012_ingest_principal_backlog.sql" {
			continue
		}
		contents, err := fs.ReadFile(migrations.SQLite(), entry.Name())
		if err != nil {
			t.Fatal(err)
		}
		prefix[entry.Name()] = &fstest.MapFile{Data: contents}
	}
	if err := control.ApplyMigrations(ctx, raw, prefix); err != nil {
		t.Fatal(err)
	}
	if _, err := raw.ExecContext(ctx, `
		INSERT INTO ingest_batch_identities VALUES ('legacy', 'legacy-sequence', zeroblob(32), 1, 1);
		INSERT INTO ingest_visibility_reservations (
			sequence, batch_key, state, phase, attempt_id, index_time_unix_milli,
			metadata, outbox, outbox_sha256, stored_row_count, decoded_event_bytes,
			created_at_unix_micro, committed_at_unix_micro
		) VALUES (1, 'legacy', 'reserved', 'unsent', 'stale-owner', 1, X'', X'78', ?, 1, 1, 1, NULL);
		UPDATE ingest_visibility_state SET last_assigned = 1 WHERE singleton = 1`, sha256Bytes([]byte("x"))); err != nil {
		t.Fatal(err)
	}
	if err := raw.Close(); err != nil {
		t.Fatal(err)
	}
	db, err := control.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	sequencer, err := NewSQLite(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sequencer.Close() })
	var owner, outbox []byte
	if err := db.SQLDB().QueryRowContext(ctx, `SELECT principal_sha256, outbox FROM ingest_visibility_reservations WHERE sequence = 1`).Scan(&owner, &outbox); err != nil {
		t.Fatal(err)
	}
	if len(owner) != 0 || string(outbox) != "x" {
		t.Fatalf("upgrade rewrote legacy authority or payload: owner=%x outbox=%q", owner, outbox)
	}
	request := reserveRequest("new-after-upgrade", "new-owner")
	seedPrincipalPending(t, db, request.PrincipalSHA256[:], MaxPrincipalPendingReservations-1, 1, 0)
	if _, err := sequencer.Reserve(ctx, request); !errors.Is(err, ErrPendingCapacity) {
		t.Fatalf("legacy debt not charged with principal usage: %v", err)
	}
	legacy, found, err := sequencer.AcquirePending(ctx, "legacy-replay")
	if err != nil || !found || legacy.Sequence != 1 || string(legacy.Outbox) != "x" {
		t.Fatalf("legacy replay = %+v found=%t, %v", legacy, found, err)
	}
	markAndCommit(t, sequencer, legacy.Sequence, "legacy-replay", testCommittedAt)
	if _, err := sequencer.Reserve(ctx, request); err != nil {
		t.Fatalf("legacy debt not freed by replay commit: %v", err)
	}
}

func TestSQLitePrincipalIsRequiredAndImmutable(t *testing.T) {
	t.Parallel()
	sequencer, db := openTestSequencer(t)
	request := reserveRequest("principal-required", "owner")
	request.PrincipalSHA256 = [sha256.Size]byte{}
	if _, err := sequencer.Reserve(context.Background(), request); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("unattributed fresh reservation accepted: %v", err)
	}
	assertQuotaObjectCounts(t, db, 0, 0, 0)
	request.PrincipalSHA256 = sha256.Sum256([]byte("known-source"))
	accepted, err := sequencer.Reserve(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	for _, replacement := range [][]byte{{}, sha256Bytes([]byte("other-source"))} {
		if _, err := db.SQLDB().ExecContext(context.Background(), `UPDATE ingest_visibility_reservations SET principal_sha256 = ? WHERE sequence = ?`, replacement, accepted.Sequence); err == nil {
			t.Fatal("persisted principal could be erased or reassigned")
		}
	}
}

func seedPrincipalPending(t *testing.T, db *control.DB, principal []byte, count, outboxBytes, metadataBytes int) {
	t.Helper()
	ctx := context.Background()
	tx, err := db.SQLDB().BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		t.Fatal(err)
	}
	defer rollback(tx)
	var last int64
	if err := tx.QueryRowContext(ctx, `SELECT last_assigned FROM ingest_visibility_state WHERE singleton = 1`).Scan(&last); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, `
		WITH RECURSIVE sequences(sequence) AS (
			SELECT ? UNION ALL SELECT sequence + 1 FROM sequences WHERE sequence < ?
		)
		INSERT INTO ingest_batch_identities
		SELECT printf('fixture-%d', sequence), printf('fixture-sequence-%d', sequence), zeroblob(32), sequence, sequence
		FROM sequences`, last+1, last+int64(count)); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO ingest_visibility_reservations (
			sequence, batch_key, state, phase, attempt_id, index_time_unix_milli,
			metadata, outbox, outbox_sha256, stored_row_count, decoded_event_bytes,
			principal_sha256, created_at_unix_micro, committed_at_unix_micro
		)
		SELECT first_visibility_seq, batch_key, 'reserved', 'unsent', '', 1,
		       zeroblob(?), zeroblob(?), ?, 1, 1, ?, first_visibility_seq, NULL
		FROM ingest_batch_identities WHERE first_visibility_seq > ?`,
		metadataBytes, outboxBytes, sha256Bytes(make([]byte, outboxBytes)), principal, last); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE ingest_visibility_state SET last_assigned = ? WHERE singleton = 1`, last+int64(count)); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func sha256Bytes(value []byte) []byte {
	digest := sha256.Sum256(value)
	return digest[:]
}
