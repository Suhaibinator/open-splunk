package visibility

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/ingestquota"
)

type scriptedHECAcknowledgmentIDSource struct {
	ids   []uint64
	next  int
	calls []struct {
		scope             hecAcknowledgmentScope
		allocationOrdinal uint64
		collisionAttempt  uint64
	}
}

func deterministicHECAcknowledgmentIDSource(seed byte) *keyedHECAcknowledgmentIDSource {
	source := &keyedHECAcknowledgmentIDSource{}
	for index := range source.key {
		source.key[index] = seed + byte(index)
	}
	return source
}

func (source *scriptedHECAcknowledgmentIDSource) ID(
	scope hecAcknowledgmentScope,
	allocationOrdinal uint64,
	collisionAttempt uint64,
) (uint64, error) {
	source.calls = append(source.calls, struct {
		scope             hecAcknowledgmentScope
		allocationOrdinal uint64
		collisionAttempt  uint64
	}{scope, allocationOrdinal, collisionAttempt})
	if source.next >= len(source.ids) {
		return 0, errors.New("scripted HEC acknowledgment IDs exhausted")
	}
	result := source.ids[source.next]
	source.next++
	return result, nil
}

func TestKeyedHECAcknowledgmentIDSourceIsExactAndConstantSpace(t *testing.T) {
	t.Parallel()
	source := deterministicHECAcknowledgmentIDSource(1)
	before := *source
	scope := hecAcknowledgmentScope{
		tenantID: "tenant-a",
		tokenID:  "token-a",
		channel:  "123e4567-e89b-42d3-a456-426614174001",
	}
	first, err := source.ID(scope, 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	second, err := source.ID(scope, 2, 0)
	if err != nil {
		t.Fatal(err)
	}
	if first == 0 || first > maximumHECAcknowledgmentID || second != first+1 {
		t.Fatalf("first keyed ACK block = %d, %d", first, second)
	}
	retried, err := source.ID(scope, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	if retried == first || retried == 0 || retried > maximumHECAcknowledgmentID {
		t.Fatalf("collision-derived ACK ID = %d, first %d", retried, first)
	}
	for index := uint64(0); index < 10_000; index++ {
		churnedScope := hecAcknowledgmentScope{
			tenantID: "tenant-a",
			tokenID:  fmt.Sprintf("token-%d", index),
			channel:  fmt.Sprintf("channel-%d", index),
		}
		if _, err := source.ID(churnedScope, index+1, index%3); err != nil {
			t.Fatalf("derive churned scope %d: %v", index, err)
		}
	}
	if *source != before {
		t.Fatal("keyed ACK ID source accumulated mutable per-scope state")
	}
}

func TestHECAcknowledgmentCollisionRerollsAtomically(t *testing.T) {
	t.Parallel()
	sequencer, db := openTestSequencer(t)
	source := &scriptedHECAcknowledgmentIDSource{ids: []uint64{515, 515, 616}}
	sequencer.hecAcknowledgmentIDs = source
	insertHECTestToken(t, db.SQLDB(), "hec-token", 1, true)

	firstRequest := reserveRequest("hec-collision-one", "hec-collision-one-attempt")
	firstRequest.HECAdmission = &HECAdmissionRequest{
		TenantID:              "tenant-a",
		TokenID:               "hec-token",
		TokenVersion:          1,
		AuthorizedIndexes:     []HECIndexAuthority{{Name: "main", Version: 1}},
		RequestID:             "hec-collision-one-request",
		Acknowledgment:        true,
		AcknowledgmentChannel: "collision-channel",
		CreatedAt:             firstRequest.IndexTime,
	}
	first, err := sequencer.Reserve(context.Background(), firstRequest)
	if err != nil || first.HECAcknowledgmentID != 515 {
		t.Fatalf("first collision fixture reservation = %+v error=%v", first, err)
	}
	secondRequest := reserveRequest("hec-collision-two", "hec-collision-two-attempt")
	secondRequest.HECAdmission = &HECAdmissionRequest{
		TenantID:              "tenant-a",
		TokenID:               "hec-token",
		TokenVersion:          1,
		AuthorizedIndexes:     []HECIndexAuthority{{Name: "main", Version: 1}},
		RequestID:             "hec-collision-two-request",
		Acknowledgment:        true,
		AcknowledgmentChannel: "collision-channel",
		CreatedAt:             secondRequest.IndexTime,
	}
	second, err := sequencer.Reserve(context.Background(), secondRequest)
	if err != nil || second.HECAcknowledgmentID != 616 {
		t.Fatalf("rerolled collision reservation = %+v error=%v", second, err)
	}
	if len(source.calls) != 3 || source.calls[1].allocationOrdinal != 2 ||
		source.calls[1].collisionAttempt != 0 || source.calls[2].allocationOrdinal != 2 ||
		source.calls[2].collisionAttempt != 1 {
		t.Fatalf("collision derivation calls = %+v", source.calls)
	}
}

func TestHECAdmissionAndAcknowledgmentAreAtomicWithVisibility(t *testing.T) {
	t.Parallel()
	sequencer, db := openTestSequencer(t)
	sequencer.hecAcknowledgmentIDs = &scriptedHECAcknowledgmentIDSource{
		ids: []uint64{101, 202},
	}
	ctx := context.Background()
	observedAt := testCommittedAt.Add(time.Hour)
	sequencer.now = func() time.Time { return observedAt }
	insertHECTestToken(t, db.SQLDB(), "hec-token", 1, true)

	request := reserveRequest("hec-request-one", "hec-attempt-one")
	request.HECAdmission = &HECAdmissionRequest{
		TenantID:              "tenant-a",
		TokenID:               "hec-token",
		TokenVersion:          1,
		AuthorizedIndexes:     []HECIndexAuthority{{Name: "main", Version: 1}},
		RequestID:             "request-one",
		Acknowledgment:        true,
		AcknowledgmentChannel: "Channel-A",
		CreatedAt:             request.IndexTime,
	}
	reservation, err := sequencer.Reserve(ctx, request)
	if err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	if reservation.HECRequestSequence != 1 || reservation.HECAcknowledgmentID != 101 {
		t.Fatalf("HEC allocation = request %d ack %d", reservation.HECRequestSequence, reservation.HECAcknowledgmentID)
	}
	health, err := sequencer.HECOperationalHealth(ctx)
	if err != nil || !health.QueueAvailable || !health.AcknowledgmentAvailable ||
		!health.RequestCapacityAvailable || health.RetainedRequests != 1 ||
		health.PendingOutboxReservations != 1 || health.ActiveChannels != 1 ||
		health.RetainedChannels != 1 ||
		health.PendingAcknowledgments != 1 || health.IndexedAcknowledgments != 0 {
		t.Fatalf("pending HEC operational health = %+v error=%v", health, err)
	}
	if _, err := db.SQLDB().ExecContext(ctx, `
		UPDATE ingest_visibility_reservations
		SET created_at_unix_micro = ?
		WHERE sequence = ?`,
		observedAt.Add(-5*time.Minute).UnixMicro(),
		reservation.Sequence,
	); err != nil {
		t.Fatal(err)
	}
	health, err = sequencer.HECOperationalHealth(ctx)
	if err != nil || health.OldestPendingOutboxAge != 5*time.Minute {
		t.Fatalf("pending HEC oldest age = %+v error=%v", health, err)
	}

	statuses, err := sequencer.LookupHECAcknowledgments(
		ctx,
		"tenant-a",
		"hec-token",
		"Channel-A",
		[]uint64{101, 99},
	)
	if err != nil || statuses[101] || statuses[99] || len(statuses) != 2 {
		t.Fatalf("pending acknowledgment statuses = %v error=%v", statuses, err)
	}
	otherChannel, err := sequencer.LookupHECAcknowledgments(
		ctx,
		"tenant-a",
		"hec-token",
		"channel-a",
		[]uint64{101},
	)
	if err != nil || otherChannel[101] {
		t.Fatalf("case-distinct channel leaked acknowledgment: %v error=%v", otherChannel, err)
	}
	for label, scope := range map[string][2]string{
		"tenant": {"tenant-b", "hec-token"},
		"token":  {"tenant-a", "other-token"},
	} {
		isolated, lookupErr := sequencer.LookupHECAcknowledgments(
			ctx,
			scope[0],
			scope[1],
			"Channel-A",
			[]uint64{101},
		)
		if lookupErr != nil || isolated[101] {
			t.Fatalf("%s scope leaked acknowledgment: %v error=%v", label, isolated, lookupErr)
		}
	}

	markAndCommit(t, sequencer, reservation.Sequence, request.AttemptID, testCommittedAt)
	statuses, err = sequencer.LookupHECAcknowledgments(
		ctx,
		"tenant-a",
		"hec-token",
		"Channel-A",
		[]uint64{101},
	)
	if err != nil || !statuses[101] {
		t.Fatalf("indexed acknowledgment status = %v error=%v", statuses, err)
	}
	health, err = sequencer.HECOperationalHealth(ctx)
	if err != nil || health.PendingOutboxReservations != 0 ||
		health.PendingAcknowledgments != 0 || health.IndexedAcknowledgments != 1 ||
		health.ExpiredAcknowledgments != 0 || health.ActiveChannels != 1 ||
		!health.QueueAvailable || !health.AcknowledgmentAvailable {
		t.Fatalf("indexed HEC operational health = %+v error=%v", health, err)
	}
	observedAt = testCommittedAt.Add(HECTerminalRetention + time.Microsecond)
	health, err = sequencer.HECOperationalHealth(ctx)
	if err != nil || health.IndexedAcknowledgments != 0 ||
		health.ExpiredAcknowledgments != 1 || health.ActiveChannels != 0 ||
		health.RetainedChannels != 1 {
		t.Fatalf("expired HEC operational health = %+v error=%v", health, err)
	}

	var requestState string
	var terminalAt int64
	if err := db.SQLDB().QueryRowContext(ctx, `
		SELECT state, terminal_at_unix_micro
		FROM hec_requests
		WHERE tenant_id = 'tenant-a'
		  AND ingestion_token_id = 'hec-token'
		  AND request_sequence = 1`).Scan(&requestState, &terminalAt); err != nil {
		t.Fatal(err)
	}
	if requestState != "indexed" || terminalAt != testCommittedAt.UnixMicro() {
		t.Fatalf("durable HEC request = state %q terminal %d", requestState, terminalAt)
	}
	deleted, err := sequencer.PruneHECTerminalRequests(
		ctx,
		testCommittedAt.Add(time.Microsecond),
		10,
	)
	if err != nil || deleted != 1 {
		t.Fatalf("PruneHECTerminalRequests() = %d, %v", deleted, err)
	}
	second := reserveRequest("hec-request-two", "hec-attempt-two")
	second.HECAdmission = &HECAdmissionRequest{
		TenantID:              "tenant-a",
		TokenID:               "hec-token",
		TokenVersion:          1,
		AuthorizedIndexes:     []HECIndexAuthority{{Name: "main", Version: 1}},
		RequestID:             "request-two",
		Acknowledgment:        true,
		AcknowledgmentChannel: "Channel-A",
		CreatedAt:             second.IndexTime,
	}
	secondReservation, err := sequencer.Reserve(ctx, second)
	if err != nil || secondReservation.HECAcknowledgmentID != 202 {
		t.Fatalf("post-prune acknowledgment = %+v error=%v, want opaque ID 202", secondReservation, err)
	}
}

func TestHECTerminalFailurePersistsAcrossRestartAndCleanupAllocatesOpaqueID(t *testing.T) {
	t.Parallel()
	sequencer, db := openTestSequencer(t)
	sequencer.hecAcknowledgmentIDs = &scriptedHECAcknowledgmentIDSource{ids: []uint64{303}}
	ctx := context.Background()
	insertHECTestToken(t, db.SQLDB(), "hec-token", 1, true)

	request := reserveRequest("hec-terminal-request", "hec-terminal-attempt")
	request.HECAdmission = &HECAdmissionRequest{
		TenantID:              "tenant-a",
		TokenID:               "hec-token",
		TokenVersion:          1,
		AuthorizedIndexes:     []HECIndexAuthority{{Name: "main", Version: 1}},
		RequestID:             "request-terminal",
		Acknowledgment:        true,
		AcknowledgmentChannel: "terminal-channel",
		CreatedAt:             request.IndexTime,
	}
	reservation, err := sequencer.Reserve(ctx, request)
	if err != nil {
		t.Fatalf("Reserve terminal HEC request: %v", err)
	}
	if err := sequencer.Abandon(ctx, reservation.Sequence, request.AttemptID); err != nil {
		t.Fatalf("Abandon terminal HEC request: %v", err)
	}
	readiness, err := sequencer.HECReadiness(ctx)
	if err != nil || !readiness.QueueAvailable || readiness.AcknowledgmentAvailable {
		t.Fatalf("terminal HEC readiness = %+v error=%v", readiness, err)
	}
	statuses, err := sequencer.LookupHECAcknowledgments(
		ctx,
		"tenant-a",
		"hec-token",
		"terminal-channel",
		[]uint64{303},
	)
	if err != nil || statuses[303] {
		t.Fatalf("terminal acknowledgment status = %v error=%v", statuses, err)
	}

	if err := sequencer.Close(); err != nil {
		t.Fatalf("close first sequencer: %v", err)
	}
	reopened, err := NewSQLite(ctx, db)
	if err != nil {
		t.Fatalf("reopen sequencer: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	reopened.hecAcknowledgmentIDs = &scriptedHECAcknowledgmentIDSource{ids: []uint64{404}}
	readiness, err = reopened.HECReadiness(ctx)
	if err != nil || readiness.AcknowledgmentAvailable {
		t.Fatalf("reopened terminal HEC readiness = %+v error=%v", readiness, err)
	}
	deleted, err := reopened.PruneHECTerminalRequests(ctx, time.Now().UTC().Add(time.Hour), 10)
	if err != nil || deleted != 1 {
		t.Fatalf("prune terminal HEC request = %d, %v", deleted, err)
	}
	readiness, err = reopened.HECReadiness(ctx)
	if err != nil || !readiness.AcknowledgmentAvailable {
		t.Fatalf("post-cleanup HEC readiness = %+v error=%v", readiness, err)
	}

	next := reserveRequest("hec-after-terminal", "hec-after-terminal-attempt")
	next.HECAdmission = &HECAdmissionRequest{
		TenantID:              "tenant-a",
		TokenID:               "hec-token",
		TokenVersion:          1,
		AuthorizedIndexes:     []HECIndexAuthority{{Name: "main", Version: 1}},
		RequestID:             "request-after-terminal",
		Acknowledgment:        true,
		AcknowledgmentChannel: "terminal-channel",
		CreatedAt:             next.IndexTime,
	}
	nextReservation, err := reopened.Reserve(ctx, next)
	if err != nil || nextReservation.HECAcknowledgmentID != 404 {
		t.Fatalf("post-restart allocation = %+v error=%v, want opaque ack ID 404", nextReservation, err)
	}
}

func TestHECChannelCapacityPrecedesQuotaAndRollsBackAdmission(t *testing.T) {
	t.Parallel()
	sequencer, db := openTestSequencer(t)
	ctx := context.Background()
	insertHECTestToken(t, db.SQLDB(), "hec-token", 1, true)
	nowMicros := time.Now().UTC().UnixMicro()
	tx, err := db.SQLDB().BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	for index := 0; index < MaxHECChannelsPerToken; index++ {
		channel := fmt.Sprintf("capacity-channel-%03d", index)
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO hec_channels (
				tenant_id, ingestion_token_id, channel_id,
				next_acknowledgment_id, created_at_unix_micro,
				last_used_at_unix_micro
			) VALUES ('tenant-a', 'hec-token', ?, 1, ?, ?)`,
			channel,
			nowMicros,
			nowMicros,
		); err != nil {
			_ = tx.Rollback()
			t.Fatalf("insert capacity channel %d: %v", index, err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	request := reserveRequest("hec-channel-capacity", "hec-channel-capacity-attempt")
	request.QuotaAdmission = &ingestquota.Admission{}
	request.QuotaEvaluatedAt = request.IndexTime
	request.HECAdmission = &HECAdmissionRequest{
		TenantID:              "tenant-a",
		TokenID:               "hec-token",
		TokenVersion:          1,
		AuthorizedIndexes:     []HECIndexAuthority{{Name: "main", Version: 1}},
		RequestID:             "request-channel-capacity",
		Acknowledgment:        true,
		AcknowledgmentChannel: "new-capacity-channel",
		CreatedAt:             request.IndexTime,
	}
	if _, err := sequencer.Reserve(ctx, request); !errors.Is(err, ErrHECAcknowledgmentCapacity) {
		t.Fatalf("channel-capacity Reserve error = %v", err)
	}
	for _, table := range []string{
		"ingest_batch_identities",
		"ingest_visibility_reservations",
		"ingest_quota_buckets",
		"hec_source_sequences",
		"hec_requests",
		"hec_acknowledgments",
	} {
		var count int
		if err := db.SQLDB().QueryRowContext(
			ctx,
			"SELECT count(*) FROM "+table, // #nosec G201 -- fixed test table names.
		).Scan(&count); err != nil {
			t.Fatalf("count %s: %v", table, err)
		}
		if count != 0 {
			t.Fatalf("%s rows = %d after capacity rejection", table, count)
		}
	}
}

func TestHECSequenceExhaustionIsAtomic(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name        string
		prepare     func(*testing.T, *sql.DB, int64)
		want        error
		acknowledge bool
	}{
		{
			name: "request sequence",
			prepare: func(t *testing.T, database *sql.DB, nowMicros int64) {
				t.Helper()
				if _, err := database.ExecContext(t.Context(), `
					INSERT INTO hec_source_sequences (
						tenant_id, ingestion_token_id, next_request_sequence,
						updated_at_unix_micro
					) VALUES ('tenant-a', 'hec-token', ?, ?)`, math.MaxInt64, nowMicros); err != nil {
					t.Fatal(err)
				}
			},
			want: ErrHECRequestCapacity,
		},
		{
			name: "acknowledgment sequence",
			prepare: func(t *testing.T, database *sql.DB, nowMicros int64) {
				t.Helper()
				if _, err := database.ExecContext(t.Context(), `
					INSERT INTO hec_channels (
						tenant_id, ingestion_token_id, channel_id,
						next_acknowledgment_id, created_at_unix_micro,
						last_used_at_unix_micro
					) VALUES ('tenant-a', 'hec-token', 'overflow-channel', ?, ?, ?)`,
					math.MaxInt64,
					nowMicros,
					nowMicros,
				); err != nil {
					t.Fatal(err)
				}
			},
			want:        ErrHECAcknowledgmentCapacity,
			acknowledge: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			sequencer, db := openTestSequencer(t)
			ctx := context.Background()
			insertHECTestToken(t, db.SQLDB(), "hec-token", 1, test.acknowledge)
			test.prepare(t, db.SQLDB(), time.Now().UTC().UnixMicro())
			request := reserveRequest("hec-overflow", "hec-overflow-attempt")
			request.HECAdmission = &HECAdmissionRequest{
				TenantID:              "tenant-a",
				TokenID:               "hec-token",
				TokenVersion:          1,
				AuthorizedIndexes:     []HECIndexAuthority{{Name: "main", Version: 1}},
				RequestID:             "request-overflow",
				Acknowledgment:        test.acknowledge,
				AcknowledgmentChannel: map[bool]string{true: "overflow-channel"}[test.acknowledge],
				CreatedAt:             request.IndexTime,
			}
			if _, err := sequencer.Reserve(ctx, request); !errors.Is(err, test.want) {
				t.Fatalf("Reserve overflow error = %v, want %v", err, test.want)
			}
			for _, table := range []string{"ingest_batch_identities", "ingest_visibility_reservations", "hec_requests", "hec_acknowledgments"} {
				var count int
				if err := db.SQLDB().QueryRowContext(ctx, "SELECT count(*) FROM "+table).Scan(&count); err != nil { // #nosec G201 -- fixed test table names.
					t.Fatal(err)
				}
				if count != 0 {
					t.Fatalf("%s rows = %d after overflow", table, count)
				}
			}
		})
	}
}

func TestHECAdmissionStaleSnapshotRollsBackEveryAllocation(t *testing.T) {
	t.Parallel()
	sequencer, db := openTestSequencer(t)
	insertHECTestToken(t, db.SQLDB(), "hec-token", 2, true)
	request := reserveRequest("stale-hec-request", "stale-hec-attempt")
	request.HECAdmission = &HECAdmissionRequest{
		TenantID:              "tenant-a",
		TokenID:               "hec-token",
		TokenVersion:          1,
		AuthorizedIndexes:     []HECIndexAuthority{{Name: "main", Version: 1}},
		RequestID:             "request-stale",
		Acknowledgment:        true,
		AcknowledgmentChannel: "channel",
		CreatedAt:             request.IndexTime,
	}
	if _, err := sequencer.Reserve(context.Background(), request); !errors.Is(err, ErrHECAdmissionStale) {
		t.Fatalf("Reserve stale error = %v", err)
	}
	for _, table := range []string{
		"ingest_batch_identities",
		"ingest_visibility_reservations",
		"hec_source_sequences",
		"hec_requests",
		"hec_channels",
		"hec_acknowledgments",
	} {
		var count int
		if err := db.SQLDB().QueryRowContext(
			context.Background(),
			"SELECT count(*) FROM "+table, // #nosec G201 -- table names are fixed test constants.
		).Scan(&count); err != nil {
			t.Fatalf("count %s: %v", table, err)
		}
		if count != 0 {
			t.Fatalf("%s rows = %d, want atomic rollback", table, count)
		}
	}
}

func TestHECAdmissionRechecksExpiryAtReserveTransactionBoundary(t *testing.T) {
	t.Parallel()
	sequencer, db := openTestSequencer(t)
	insertHECTestToken(t, db.SQLDB(), "hec-expiring-token", 1, false)
	expiresAt := time.Date(2026, time.August, 10, 12, 0, 1, 0, time.UTC)
	if _, err := db.SQLDB().ExecContext(context.Background(), `
		UPDATE ingestion_tokens
		SET expires_at_unix_micro = ?
		WHERE ingestion_token_id = 'hec-expiring-token'`, expiresAt.UnixMicro()); err != nil {
		t.Fatal(err)
	}
	// The request was received and authenticated just before expiry, but the
	// durable transaction begins just after it. Receive time must never extend
	// credential authority across this boundary.
	sequencer.now = func() time.Time { return expiresAt.Add(time.Microsecond) }
	request := reserveRequest("hec-expiry-race", "hec-expiry-race-attempt")
	request.HECAdmission = &HECAdmissionRequest{
		TenantID:          "tenant-a",
		TokenID:           "hec-expiring-token",
		TokenVersion:      1,
		AuthorizedIndexes: []HECIndexAuthority{{Name: "main", Version: 1}},
		RequestID:         "request-before-expiry",
		CreatedAt:         expiresAt.Add(-time.Microsecond),
	}
	if _, err := sequencer.Reserve(context.Background(), request); !errors.Is(err, ErrHECAdmissionStale) {
		t.Fatalf("Reserve after token expiry error = %v, want %v", err, ErrHECAdmissionStale)
	}
	for _, table := range []string{
		"ingest_batch_identities",
		"ingest_visibility_reservations",
		"hec_source_sequences",
		"hec_requests",
	} {
		var count int
		if err := db.SQLDB().QueryRowContext(
			context.Background(),
			"SELECT count(*) FROM "+table, // #nosec G201 -- fixed test table names.
		).Scan(&count); err != nil {
			t.Fatalf("count %s: %v", table, err)
		}
		if count != 0 {
			t.Fatalf("%s rows = %d after expiry rejection", table, count)
		}
	}
}

func TestHECAcknowledgmentLookupRejectsDuplicateIDs(t *testing.T) {
	t.Parallel()
	sequencer, _ := openTestSequencer(t)
	if _, err := sequencer.LookupHECAcknowledgments(
		context.Background(),
		"tenant-a",
		"token-a",
		"channel-a",
		[]uint64{1, 1},
	); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("duplicate lookup error = %v", err)
	}
}

func insertHECTestToken(
	t *testing.T,
	db interface {
		ExecContext(context.Context, string, ...any) (sql.Result, error)
	},
	tokenID string,
	version uint64,
	acknowledgment bool,
) {
	t.Helper()
	now := time.Date(2026, time.July, 21, 0, 0, 0, 0, time.UTC).UnixMicro()
	digest := sha256.Sum256([]byte("test-token:" + tokenID))
	ack := 0
	if acknowledgment {
		ack = 1
	}
	if _, err := db.ExecContext(context.Background(), `
		INSERT INTO ingestion_tokens (
			ingestion_token_id, version, name, description,
			token_prefix, token_digest, state,
			created_at_unix_micro, updated_at_unix_micro,
			expires_at_unix_micro, revoked_at_unix_micro,
			last_used_at_unix_micro, bound_collector_id,
			max_ingest_events_per_second,
			max_ingest_uncompressed_bytes_per_second,
			purpose
		) VALUES (?, ?, 'HEC test token', '', 'hectest00', ?, 'active',
		          ?, ?, NULL, NULL, NULL, NULL, 0, 0, 'hec')`,
		tokenID,
		version,
		digest[:],
		now,
		now,
	); err != nil {
		t.Fatalf("insert HEC token: %v", err)
	}
	if _, err := db.ExecContext(context.Background(), `
		INSERT INTO ingestion_token_hec_profiles (
			ingestion_token_id, default_index_id, default_host,
			default_source, default_sourcetype, indexer_acknowledgment
		) VALUES (?, NULL, NULL, NULL, NULL, ?)`,
		tokenID,
		ack,
	); err != nil {
		t.Fatalf("insert HEC profile: %v", err)
	}
	if _, err := db.ExecContext(context.Background(), `
		INSERT OR IGNORE INTO indexes (
			index_id, version, name, display_name, ingestion_enabled,
			search_enabled, state, created_at_unix_micro, updated_at_unix_micro
		) VALUES ('hec-test-index', 1, 'main', 'Main', 1, 1, 'active', ?, ?)`,
		now,
		now,
	); err != nil {
		t.Fatalf("insert HEC test index: %v", err)
	}
	if _, err := db.ExecContext(context.Background(), `
		INSERT INTO ingestion_token_indexes (ingestion_token_id, index_id)
		VALUES (?, 'hec-test-index')`,
		tokenID,
	); err != nil {
		t.Fatalf("insert HEC token index scope: %v", err)
	}
}
