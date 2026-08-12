package visibility

import (
	"bytes"
	"context"
	"database/sql"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/control"
)

func TestHECDurableStateSurvivesControlBackupRestore(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	directory := t.TempDir()
	source, err := control.Open(ctx, filepath.Join(directory, "source.sqlite"))
	if err != nil {
		t.Fatalf("open source control database: %v", err)
	}
	t.Cleanup(func() { _ = source.Close() })
	sourceSequencer, err := NewSQLite(ctx, source)
	if err != nil {
		t.Fatalf("open source visibility sequencer: %v", err)
	}
	t.Cleanup(func() { _ = sourceSequencer.Close() })
	sourceSequencer.hecAcknowledgmentIDs = &scriptedHECAcknowledgmentIDSource{
		ids: []uint64{1001, 1002, 1003},
	}
	insertHECTestToken(t, source.SQLDB(), "hec-backup-token", 1, true)

	pendingRequest := hecRecoveryRequest(
		"backup-pending",
		"backup-pending-attempt",
		"backup-pending-request",
	)
	pending, err := sourceSequencer.Reserve(ctx, pendingRequest)
	if err != nil {
		t.Fatalf("reserve pending HEC request: %v", err)
	}
	indexedRequest := hecRecoveryRequest(
		"backup-indexed",
		"backup-indexed-attempt",
		"backup-indexed-request",
	)
	indexed, err := sourceSequencer.Reserve(ctx, indexedRequest)
	if err != nil {
		t.Fatalf("reserve indexed HEC request: %v", err)
	}
	markAndCommit(
		t,
		sourceSequencer,
		indexed.Sequence,
		indexedRequest.AttemptID,
		testCommittedAt,
	)
	failedRequest := hecRecoveryRequest(
		"backup-failed",
		"backup-failed-attempt",
		"backup-failed-request",
	)
	failed, err := sourceSequencer.Reserve(ctx, failedRequest)
	if err != nil {
		t.Fatalf("reserve terminal-failure HEC request: %v", err)
	}
	if err := sourceSequencer.Abandon(ctx, failed.Sequence, failedRequest.AttemptID); err != nil {
		t.Fatalf("abandon terminal-failure HEC request: %v", err)
	}

	if pending.HECRequestSequence != 1 || pending.HECAcknowledgmentID != 1001 ||
		indexed.HECRequestSequence != 2 || indexed.HECAcknowledgmentID != 1002 ||
		failed.HECRequestSequence != 3 || failed.HECAcknowledgmentID != 1003 {
		t.Fatalf(
			"source HEC allocation = pending(%d,%d) indexed(%d,%d) failed(%d,%d)",
			pending.HECRequestSequence,
			pending.HECAcknowledgmentID,
			indexed.HECRequestSequence,
			indexed.HECAcknowledgmentID,
			failed.HECRequestSequence,
			failed.HECAcknowledgmentID,
		)
	}
	if err := sourceSequencer.Close(); err != nil {
		t.Fatalf("stop source visibility sequencer before backup: %v", err)
	}

	wantRequests := readHECRecoveryRequests(t, source.SQLDB())
	wantChannel := readHECRecoveryChannel(t, source.SQLDB())
	wantPendingOutbox := readHECRecoveryOutbox(t, source.SQLDB(), pending.Sequence)
	if !bytes.Equal(wantPendingOutbox, pendingRequest.Outbox) {
		t.Fatalf("source pending outbox = %q, want %q", wantPendingOutbox, pendingRequest.Outbox)
	}
	if wantChannel.allocationOrdinal != 4 {
		t.Fatalf("source channel allocation ordinal = %d, want 4", wantChannel.allocationOrdinal)
	}

	backupPath := filepath.Join(directory, "restored.sqlite")
	if err := source.BackupTo(ctx, backupPath); err != nil {
		t.Fatalf("back up HEC control state: %v", err)
	}
	branchSequencer, err := NewSQLite(ctx, source)
	if err != nil {
		t.Fatalf("reopen source visibility sequencer after snapshot: %v", err)
	}
	branchSequencer.hecAcknowledgmentIDs = deterministicHECAcknowledgmentIDSource(11)
	lostBranchRequest := hecRecoveryRequest(
		"backup-lost-branch",
		"backup-lost-branch-attempt",
		"backup-lost-branch-request",
	)
	lostBranch, err := branchSequencer.Reserve(ctx, lostBranchRequest)
	if err != nil {
		t.Fatalf("reserve post-snapshot HEC request: %v", err)
	}
	if lostBranch.HECRequestSequence != 4 || lostBranch.HECAcknowledgmentID == 0 ||
		lostBranch.HECAcknowledgmentID > maximumHECAcknowledgmentID {
		t.Fatalf("post-snapshot source allocation = %+v, want request 4 and an exact opaque ACK", lostBranch)
	}
	if err := branchSequencer.Close(); err != nil {
		t.Fatalf("close post-snapshot source sequencer: %v", err)
	}
	restored, err := control.Open(ctx, backupPath)
	if err != nil {
		t.Fatalf("open restored HEC control state: %v", err)
	}
	t.Cleanup(func() { _ = restored.Close() })
	if err := restored.VerifyIntegrity(ctx); err != nil {
		t.Fatalf("verify restored HEC control state: %v", err)
	}
	restoredSequencer, err := NewSQLite(ctx, restored)
	if err != nil {
		t.Fatalf("open restored visibility sequencer: %v", err)
	}
	t.Cleanup(func() { _ = restoredSequencer.Close() })
	restoredSequencer.hecAcknowledgmentIDs = deterministicHECAcknowledgmentIDSource(22)

	if got := readHECRecoveryRequests(t, restored.SQLDB()); !reflect.DeepEqual(got, wantRequests) {
		t.Fatalf("restored HEC requests = %+v, want %+v", got, wantRequests)
	}
	if got := readHECRecoveryChannel(t, restored.SQLDB()); got != wantChannel {
		t.Fatalf("restored HEC channel = %+v, want %+v", got, wantChannel)
	}
	if got := readHECRecoveryOutbox(t, restored.SQLDB(), pending.Sequence); !bytes.Equal(got, wantPendingOutbox) {
		t.Fatalf("restored pending outbox = %q, want %q", got, wantPendingOutbox)
	}
	assertHECRecoveryInteger(t, restored.SQLDB(), 4, `
		SELECT next_request_sequence
		FROM hec_source_sequences
		WHERE tenant_id = 'tenant-a'
		  AND ingestion_token_id = 'hec-backup-token'`)
	assertHECRecoveryInteger(t, restored.SQLDB(), 1, `
		SELECT count(*)
		FROM ingest_visibility_reservations
		WHERE sequence = ?
		  AND state = 'reserved'
		  AND phase = 'unsent'
		  AND attempt_id = ''
		  AND length(outbox) > 0`, pending.Sequence)

	statuses, err := restoredSequencer.LookupHECAcknowledgments(
		ctx,
		"tenant-a",
		"hec-backup-token",
		"backup-channel",
		[]uint64{1001, 1002, 1003},
	)
	if err != nil {
		t.Fatalf("look up restored acknowledgments: %v", err)
	}
	if statuses[1001] || !statuses[1002] || statuses[1003] {
		t.Fatalf("restored acknowledgment states = %v, want false/true/false", statuses)
	}

	nextRequest := hecRecoveryRequest(
		"backup-next",
		"backup-next-attempt",
		"backup-next-request",
	)
	next, err := restoredSequencer.Reserve(ctx, nextRequest)
	if err != nil {
		t.Fatalf("reserve after HEC restore: %v", err)
	}
	if next.HECRequestSequence != 4 || next.HECAcknowledgmentID == 0 ||
		next.HECAcknowledgmentID > maximumHECAcknowledgmentID ||
		next.HECAcknowledgmentID == lostBranch.HECAcknowledgmentID {
		t.Fatalf(
			"post-restore allocation = request %d acknowledgment %d, want request 4 and a branch-independent exact ACK",
			next.HECRequestSequence,
			next.HECAcknowledgmentID,
		)
	}
	lostStatus, err := restoredSequencer.LookupHECAcknowledgments(
		ctx,
		"tenant-a",
		"hec-backup-token",
		"backup-channel",
		[]uint64{lostBranch.HECAcknowledgmentID},
	)
	if err != nil || lostStatus[lostBranch.HECAcknowledgmentID] {
		t.Fatalf("discarded-branch acknowledgment status = %v error=%v, want false", lostStatus, err)
	}
	markAndCommit(t, restoredSequencer, next.Sequence, nextRequest.AttemptID, testCommittedAt.Add(time.Second))
	branchStatuses, err := restoredSequencer.LookupHECAcknowledgments(
		ctx,
		"tenant-a",
		"hec-backup-token",
		"backup-channel",
		[]uint64{lostBranch.HECAcknowledgmentID, next.HECAcknowledgmentID},
	)
	if err != nil || branchStatuses[lostBranch.HECAcknowledgmentID] ||
		!branchStatuses[next.HECAcknowledgmentID] {
		t.Fatalf("post-commit branch acknowledgment states = %v error=%v, want discarded=false restored=true", branchStatuses, err)
	}
	assertHECRecoveryInteger(t, restored.SQLDB(), 5, `
		SELECT next_acknowledgment_id
		FROM hec_channels
		WHERE tenant_id = 'tenant-a'
		  AND ingestion_token_id = 'hec-backup-token'
		  AND channel_id = 'backup-channel'`)
}

func hecRecoveryRequest(batchKey, attemptID, requestID string) ReserveRequest {
	request := reserveRequest(batchKey, attemptID)
	request.HECAdmission = &HECAdmissionRequest{
		TenantID:              "tenant-a",
		TokenID:               "hec-backup-token",
		TokenVersion:          1,
		AuthorizedIndexes:     []HECIndexAuthority{{Name: "main", Version: 1}},
		RequestID:             requestID,
		Acknowledgment:        true,
		AcknowledgmentChannel: "backup-channel",
		CreatedAt:             request.IndexTime,
	}
	return request
}

type hecRecoveryRequestRow struct {
	RequestSequence    int64
	RequestID          string
	VisibilitySequence sql.NullInt64
	State              string
	CreatedAt          int64
	TerminalAt         sql.NullInt64
}

func readHECRecoveryRequests(t *testing.T, db *sql.DB) []hecRecoveryRequestRow {
	t.Helper()
	rows, err := db.QueryContext(context.Background(), `
		SELECT request_sequence, request_id, visibility_sequence, state,
		       created_at_unix_micro, terminal_at_unix_micro
		FROM hec_requests
		WHERE tenant_id = 'tenant-a'
		  AND ingestion_token_id = 'hec-backup-token'
		ORDER BY request_sequence`)
	if err != nil {
		t.Fatalf("read HEC recovery requests: %v", err)
	}
	defer rows.Close()
	var result []hecRecoveryRequestRow
	for rows.Next() {
		var row hecRecoveryRequestRow
		if err := rows.Scan(
			&row.RequestSequence,
			&row.RequestID,
			&row.VisibilitySequence,
			&row.State,
			&row.CreatedAt,
			&row.TerminalAt,
		); err != nil {
			t.Fatalf("scan HEC recovery request: %v", err)
		}
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate HEC recovery requests: %v", err)
	}
	if len(result) != 3 ||
		result[0].State != "pending" ||
		result[1].State != "indexed" ||
		result[2].State != "terminal_failure" {
		t.Fatalf("HEC recovery request states = %+v", result)
	}
	return result
}

type hecRecoveryChannelRow struct {
	allocationOrdinal int64
	createdAt         int64
	lastUsedAt        int64
}

func readHECRecoveryChannel(t *testing.T, db *sql.DB) hecRecoveryChannelRow {
	t.Helper()
	var result hecRecoveryChannelRow
	if err := db.QueryRowContext(context.Background(), `
		SELECT next_acknowledgment_id, created_at_unix_micro,
		       last_used_at_unix_micro
		FROM hec_channels
		WHERE tenant_id = 'tenant-a'
		  AND ingestion_token_id = 'hec-backup-token'
		  AND channel_id = 'backup-channel'`).Scan(
		&result.allocationOrdinal,
		&result.createdAt,
		&result.lastUsedAt,
	); err != nil {
		t.Fatalf("read HEC recovery channel: %v", err)
	}
	return result
}

func readHECRecoveryOutbox(t *testing.T, db *sql.DB, sequence uint64) []byte {
	t.Helper()
	var result []byte
	if err := db.QueryRowContext(context.Background(), `
		SELECT outbox
		FROM ingest_visibility_reservations
		WHERE sequence = ?`, sequence).Scan(&result); err != nil {
		t.Fatalf("read HEC recovery outbox: %v", err)
	}
	return result
}

func assertHECRecoveryInteger(
	t *testing.T,
	db *sql.DB,
	want int64,
	query string,
	args ...any,
) {
	t.Helper()
	var got int64
	if err := db.QueryRowContext(context.Background(), query, args...).Scan(&got); err != nil {
		t.Fatalf("read HEC recovery integer: %v", err)
	}
	if got != want {
		t.Fatalf("HEC recovery integer = %d, want %d", got, want)
	}
}
