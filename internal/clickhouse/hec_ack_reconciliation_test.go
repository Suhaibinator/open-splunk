package clickhouse

import (
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"path/filepath"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/ingest"
	"github.com/Suhaibinator/open-splunk/internal/visibility"
)

func TestHECAcknowledgmentStaysPendingAcrossAmbiguousSendUntilReconciliation(t *testing.T) {
	ctx := context.Background()
	controlDB, err := control.Open(ctx, filepath.Join(t.TempDir(), "control.sqlite"))
	if err != nil {
		t.Fatalf("open control database: %v", err)
	}
	t.Cleanup(func() { _ = controlDB.Close() })
	sequencer, err := visibility.NewSQLite(ctx, controlDB)
	if err != nil {
		t.Fatalf("open visibility sequencer: %v", err)
	}
	t.Cleanup(func() { _ = sequencer.Close() })
	seedHECAcknowledgmentAuthority(t, ctx, controlDB)

	connection := &fakeStoreConnection{}
	store := mustTestStoreWithVisibility(t, connection, fixedRetention(time.Hour), sequencer)
	t.Cleanup(func() { _ = store.Close() })
	committedAt := time.Date(2026, time.July, 21, 1, 3, 0, 0, time.UTC)
	store.clock = func() time.Time { return committedAt }

	batch := validStoreBatch()
	batch.Source = ingest.HECSource("hec-ack-token")
	batch.CollectorID = ""
	batch.BatchID = "hec-ambiguous-request"
	batch.BatchSequence = 1
	batch.SourceBatchSHA256 = testSourceBatchDigest(batch.BatchID)
	batch.Events[0].Source = batch.Source
	batch.Events[0].CollectorID = ""
	batch.Events[0].BatchID = batch.BatchID
	batch.Events[0].Event.EventId = "hec-ambiguous-event"
	batch.HECAdmission = &ingest.HECStageAdmission{
		TokenID:               "hec-ack-token",
		TokenVersion:          1,
		AuthorizedIndexes:     []ingest.HECIndexAuthority{{Name: "main", Version: 1}},
		RequestID:             batch.BatchID,
		AcknowledgmentEnabled: true,
		Channel:               "ambiguous-channel",
		CreatedAt:             batch.ReceivedAt,
	}

	staged, err := store.Stage(ctx, batch)
	if err != nil {
		t.Fatalf("stage HEC request: %v", err)
	}
	if staged.State != ingest.StoredBatchPending || staged.VisibilitySequence == 0 ||
		staged.HECRequestSequence == 0 || staged.HECAcknowledgmentID == 0 {
		t.Fatalf("staged HEC request = %+v, want durable pending request and acknowledgment", staged)
	}
	assertDurableHECAcknowledgment(t, ctx, sequencer, staged.HECAcknowledgmentID, false)

	ambiguousBatch := &fakeWriteBatch{sendErr: io.ErrUnexpectedEOF}
	connection.batch = ambiguousBatch
	if err := store.ReconcilePending(ctx); !errors.Is(err, io.ErrUnexpectedEOF) || !isTransient(err) {
		t.Fatalf("ambiguous reconciliation error = %v, want transient send failure", err)
	}
	if ambiguousBatch.sendCalls != 1 || ambiguousBatch.abortCalls != 1 {
		t.Fatalf(
			"ambiguous ClickHouse batch lifecycle = send %d abort %d, want 1/1",
			ambiguousBatch.sendCalls,
			ambiguousBatch.abortCalls,
		)
	}
	assertDurableHECAcknowledgment(t, ctx, sequencer, staged.HECAcknowledgmentID, false)
	assertHECAcknowledgmentLedgerState(
		t,
		ctx,
		controlDB,
		staged.HECAcknowledgmentID,
		"pending",
		"reserved",
		"ambiguous",
		true,
	)

	successfulBatch := &fakeWriteBatch{}
	connection.batch = successfulBatch
	if err := store.ReconcilePending(ctx); err != nil {
		t.Fatalf("reconcile ambiguous HEC request: %v", err)
	}
	if successfulBatch.sendCalls != 1 || successfulBatch.abortCalls != 0 ||
		successfulBatch.closeCalls != 1 || len(successfulBatch.rows) != 1 ||
		successfulBatch.rows[0][eventVisibilitySequenceColumn] != staged.VisibilitySequence {
		t.Fatalf("successful replay batch = %+v, want one committed row for sequence %d", successfulBatch, staged.VisibilitySequence)
	}
	assertDurableHECAcknowledgment(t, ctx, sequencer, staged.HECAcknowledgmentID, true)
	assertHECAcknowledgmentLedgerState(
		t,
		ctx,
		controlDB,
		staged.HECAcknowledgmentID,
		"indexed",
		"committed",
		"final",
		false,
	)
	telemetry := store.HECReconciliationTelemetry()
	if !telemetry.Available || telemetry.Successes != 1 || telemetry.Ambiguities != 1 {
		t.Fatalf("HEC reconciliation telemetry = %+v, want one ambiguous replay success", telemetry)
	}
}

func seedHECAcknowledgmentAuthority(t *testing.T, ctx context.Context, controlDB *control.DB) {
	t.Helper()
	nowMicros := time.Date(2026, time.July, 21, 0, 0, 0, 0, time.UTC).UnixMicro()
	tokenDigest := sha256.Sum256([]byte("HEC ambiguous acknowledgment token"))
	for _, statement := range []struct {
		query     string
		arguments []any
	}{
		{`
			INSERT INTO indexes (
				index_id, version, name, display_name, ingestion_enabled,
				search_enabled, state, created_at_unix_micro, updated_at_unix_micro
			) VALUES ('hec-ack-index', 1, 'main', 'Main', 1, 1, 'active', ?, ?)`, []any{nowMicros, nowMicros}},
		{`
			INSERT INTO ingestion_tokens (
				ingestion_token_id, version, name, description,
				token_prefix, token_digest, state,
				created_at_unix_micro, updated_at_unix_micro,
				expires_at_unix_micro, revoked_at_unix_micro,
				last_used_at_unix_micro, bound_collector_id,
				max_ingest_events_per_second,
				max_ingest_uncompressed_bytes_per_second, purpose
			) VALUES (
				'hec-ack-token', 1, 'HEC ACK', '', 'hecack00', ?, 'active',
				?, ?, NULL, NULL, NULL, NULL, 0, 0, 'hec'
			)`, []any{tokenDigest[:], nowMicros, nowMicros}},
		{`
			INSERT INTO ingestion_token_indexes (ingestion_token_id, index_id)
			VALUES ('hec-ack-token', 'hec-ack-index')`, nil},
		{`
			INSERT INTO ingestion_token_hec_profiles (
				ingestion_token_id, default_index_id, default_host,
				default_source, default_sourcetype, indexer_acknowledgment
			) VALUES ('hec-ack-token', 'hec-ack-index', NULL, NULL, NULL, 1)`, nil},
	} {
		if _, err := controlDB.SQLDB().ExecContext(ctx, statement.query, statement.arguments...); err != nil {
			t.Fatalf("seed HEC acknowledgment authority: %v", err)
		}
	}
}

func assertDurableHECAcknowledgment(
	t *testing.T,
	ctx context.Context,
	sequencer *visibility.SQLiteSequencer,
	acknowledgmentID uint64,
	want bool,
) {
	t.Helper()
	statuses, err := sequencer.LookupHECAcknowledgments(
		ctx,
		"tenant",
		"hec-ack-token",
		"ambiguous-channel",
		[]uint64{acknowledgmentID},
	)
	if err != nil {
		t.Fatalf("look up HEC acknowledgment %d: %v", acknowledgmentID, err)
	}
	if got, exists := statuses[acknowledgmentID]; !exists || got != want {
		t.Fatalf("HEC acknowledgment %d status = %v (present %v), want %v", acknowledgmentID, got, exists, want)
	}
}

func assertHECAcknowledgmentLedgerState(
	t *testing.T,
	ctx context.Context,
	controlDB *control.DB,
	acknowledgmentID uint64,
	wantHECState string,
	wantVisibilityState string,
	wantVisibilityPhase string,
	wantOutbox bool,
) {
	t.Helper()
	var hecState, visibilityState, visibilityPhase, attemptID string
	var outboxBytes int
	if err := controlDB.SQLDB().QueryRowContext(ctx, `
		SELECT request.state, reservation.state, reservation.phase,
		       reservation.attempt_id, length(reservation.outbox)
		FROM hec_acknowledgments AS acknowledgment
		JOIN hec_requests AS request
		  ON request.tenant_id = acknowledgment.tenant_id
		 AND request.ingestion_token_id = acknowledgment.ingestion_token_id
		 AND request.request_sequence = acknowledgment.request_sequence
		JOIN ingest_visibility_reservations AS reservation
		  ON reservation.sequence = request.visibility_sequence
		WHERE acknowledgment.tenant_id = 'tenant'
		  AND acknowledgment.ingestion_token_id = 'hec-ack-token'
		  AND acknowledgment.channel_id = 'ambiguous-channel'
		  AND acknowledgment.acknowledgment_id = ?`, int64(acknowledgmentID)).Scan(
		&hecState,
		&visibilityState,
		&visibilityPhase,
		&attemptID,
		&outboxBytes,
	); err != nil {
		t.Fatalf("read durable HEC acknowledgment %d: %v", acknowledgmentID, err)
	}
	if hecState != wantHECState || visibilityState != wantVisibilityState ||
		visibilityPhase != wantVisibilityPhase || attemptID != "" || (outboxBytes > 0) != wantOutbox {
		t.Fatalf(
			"durable HEC acknowledgment %d = HEC %q visibility %q/%q attempt %q outbox %d; want %q %q/%q empty attempt outbox=%v",
			acknowledgmentID,
			hecState,
			visibilityState,
			visibilityPhase,
			attemptID,
			outboxBytes,
			wantHECState,
			wantVisibilityState,
			wantVisibilityPhase,
			wantOutbox,
		)
	}
}
