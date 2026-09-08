package visibility

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/ingestquota"
)

func rejectionQuotaRequest(name string, now time.Time, token string, limits ingestquota.Limits) RejectRequest {
	request := rejectRequest(name)
	request.QuotaEvaluatedAt = now
	request.RejectionAdmission = &ingestquota.RejectionAdmission{
		Scope:       ingestquota.ScopeKey{Kind: ingestquota.ScopeKindToken, TenantID: "tenant-a", Identity: token},
		TokenLimits: limits,
	}
	return request
}

func TestSQLiteRejectionQuotaBoundsConcurrentWritesAndPreservesReplayAcrossRestart(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "control.sqlite")
	database, err := control.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	sequencer, err := NewSQLite(ctx, database)
	if err != nil {
		t.Fatal(err)
	}
	now := testRejectedAt
	seedQuotaToken(t, database, "token-a")
	limits := ingestquota.Limits{MaxEventsPerSecond: 1}
	const workers = 16
	start := make(chan struct{})
	var group sync.WaitGroup
	requests := make([]RejectRequest, workers)
	results := make([]Reservation, workers)
	errs := make([]error, workers)
	for index := range workers {
		requests[index] = rejectionQuotaRequest(fmt.Sprintf("rejected-%d", index), now, "token-a", limits)
		group.Go(func() {
			<-start
			results[index], errs[index] = sequencer.Reject(ctx, requests[index])
		})
	}
	close(start)
	group.Wait()
	var winner RejectRequest
	var receipt Reservation
	admitted := 0
	for index, err := range errs {
		if err == nil {
			admitted++
			winner, receipt = requests[index], results[index]
			continue
		}
		var exceeded *ingestquota.ExceededError
		if !errors.As(err, &exceeded) || exceeded.Scope != requests[index].RejectionAdmission.Scope || exceeded.RetryAfter != time.Second {
			t.Fatalf("concurrent Reject %d = %v", index, err)
		}
	}
	if admitted != 1 || !receipt.NewlyRejected {
		t.Fatalf("fresh terminal receipts = %d, want 1", admitted)
	}
	assertQuotaObjectCounts(t, database, 1, 1, 0)
	assertCutoff(t, sequencer, 1)
	before := readRejectionQuota(t, database, "token-a")
	// Exact duplicate races bypass even a now-invalid mutable budget snapshot.
	winner.RejectionAdmission = &ingestquota.RejectionAdmission{}
	winner.QuotaEvaluatedAt = time.Time{}
	for range 2 {
		got, err := sequencer.Reject(ctx, winner)
		if err != nil || got.NewlyRejected || !sameDurableReservation(got, receipt) {
			t.Fatalf("exact replay = %+v, %v", got, err)
		}
	}
	if err := sequencer.Close(); err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	database, err = control.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	sequencer, err = NewSQLite(ctx, database)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sequencer.Close() })
	got, err := sequencer.Reject(ctx, winner)
	if err != nil || !sameDurableReservation(got, receipt) {
		t.Fatalf("replay after restart = %+v, %v", got, err)
	}
	blocked := rejectionQuotaRequest("still-blocked", now, "token-a", limits)
	if _, err := sequencer.Reject(ctx, blocked); !isRejectionQuotaExceeded(err) {
		t.Fatalf("restart erased rejection debt: %v", err)
	}
	if after := readRejectionQuota(t, database, "token-a"); after != before {
		t.Fatalf("replay or denial mutated quota: before=%+v after=%+v", before, after)
	}
	// Accepted quota is independent even while the rejection budget is depleted.
	accepted := quotaReserveRequest("legitimate", "attempt", now, blocked.RejectionAdmission.Scope, limits, ingestquota.Limits{})
	if _, err := sequencer.Reserve(ctx, accepted); err != nil {
		t.Fatalf("rejections exhausted legitimate accepted quota: %v", err)
	}
	if _, err := database.SQLDB().ExecContext(ctx, `
		INSERT INTO ingestion_tokens (
			ingestion_token_id, version, name, token_prefix, token_digest,
			state, created_at_unix_micro, updated_at_unix_micro, bound_collector_id
		) VALUES ('token-b', 1, 'other token', 'other123', randomblob(32),
			'active', 1, 1, 'collector-b')`); err != nil {
		t.Fatal(err)
	}
	other := rejectionQuotaRequest("other-token", now, "token-b", limits)
	if _, err := sequencer.Reject(ctx, other); err != nil {
		t.Fatalf("rejections exhausted another token: %v", err)
	}
	blocked.QuotaEvaluatedAt = now.Add(time.Second)
	if _, err := sequencer.Reject(ctx, blocked); err != nil {
		t.Fatalf("retry at exact boundary: %v", err)
	}
}

func TestSQLiteRejectionQuotaChargesMetadataAndRollsBackFailures(t *testing.T) {
	t.Parallel()
	sequencer, database := openTestSequencer(t)
	ctx := context.Background()
	seedQuotaToken(t, database, "token-a")
	now := testRejectedAt
	request := rejectionQuotaRequest("metadata", now, "token-a", ingestquota.Limits{MaxUncompressedBytesPerSecond: 10})
	request.Metadata = make([]byte, 20)
	// A failure after sequence/identity allocation must roll the entire charge back.
	if _, err := database.SQLDB().ExecContext(ctx, `
		CREATE TRIGGER fail_rejection_budget BEFORE INSERT ON ingest_rejection_buckets
		BEGIN SELECT RAISE(ABORT, 'test rollback'); END`); err != nil {
		t.Fatal(err)
	}
	if _, err := sequencer.Reject(ctx, request); err == nil {
		t.Fatal("injected budget failure succeeded")
	}
	assertQuotaObjectCounts(t, database, 0, 0, 0)
	assertCutoff(t, sequencer, 0)
	if _, err := database.SQLDB().ExecContext(ctx, `DROP TRIGGER fail_rejection_budget`); err != nil {
		t.Fatal(err)
	}
	first, err := sequencer.Reject(ctx, request)
	if err != nil || first.Sequence != 1 {
		t.Fatalf("retry after rollback = %+v, %v", first, err)
	}
	before := readRejectionQuota(t, database, "token-a")
	next := rejectionQuotaRequest("metadata-next", now.Add(time.Second), "token-a", request.RejectionAdmission.TokenLimits)
	_, err = sequencer.Reject(ctx, next)
	var exceeded *ingestquota.ExceededError
	if !errors.As(err, &exceeded) || exceeded.RetryAfter != time.Second {
		t.Fatalf("metadata budget denial = %v", err)
	}
	if after := readRejectionQuota(t, database, "token-a"); after != before {
		t.Fatalf("metadata denial mutated quota: %+v", after)
	}
	assertQuotaObjectCounts(t, database, 1, 1, 0)
	// Even aggressive receipt retention must not replenish the token budget.
	if deleted, err := sequencer.PruneTerminal(ctx, TerminalRetention{Committed: 1, Rejected: 1, RejectedMetadataBytes: 1}, MaxPruneLimit); err != nil || deleted != 1 {
		t.Fatalf("prune receipt = %d, %v", deleted, err)
	}
	if _, err := sequencer.Reject(ctx, next); !isRejectionQuotaExceeded(err) {
		t.Fatalf("receipt pruning erased rejection debt: %v", err)
	}
	assertQuotaObjectCounts(t, database, 0, 0, 0)
	if _, err := database.SQLDB().ExecContext(ctx, `DELETE FROM ingestion_tokens WHERE ingestion_token_id = 'token-a'`); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := database.SQLDB().QueryRowContext(ctx, `SELECT count(*) FROM ingest_rejection_buckets`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("token deletion left rejection buckets: %d, %v", count, err)
	}
}

func TestSQLiteRejectionQuotaRejectsInvalidAdmissionWithoutWrites(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"index-scope", "missing-token", "invalid-policy", "invalid-time"} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			sequencer, database := openTestSequencer(t)
			request := rejectionQuotaRequest("invalid", testRejectedAt, "token-a", ingestquota.Limits{})
			switch kind {
			case "index-scope":
				request.RejectionAdmission.Scope.Kind = ingestquota.ScopeKindIndex
			case "missing-token":
				request.RejectionAdmission.Scope.Identity = ""
			case "invalid-policy":
				request.RejectionAdmission.TokenLimits.MaxEventsPerSecond = ingestquota.HardMaxEventsPerSecond + 1
			case "invalid-time":
				request.QuotaEvaluatedAt = time.Time{}
			}
			if _, err := sequencer.Reject(context.Background(), request); !errors.Is(err, ErrInvalidArgument) {
				t.Fatalf("invalid admission = %v", err)
			}
			assertQuotaObjectCounts(t, database, 0, 0, 0)
			assertCutoff(t, sequencer, 0)
		})
	}
}

func TestSQLiteRejectionQuotaPreservesWinningPendingAndCommittedOutcomes(t *testing.T) {
	t.Parallel()
	for _, committed := range []bool{false, true} {
		t.Run(fmt.Sprintf("committed-%t", committed), func(t *testing.T) {
			t.Parallel()
			sequencer, database := openTestSequencer(t)
			ctx := context.Background()
			seedQuotaToken(t, database, "token-a")
			initial := rejectionQuotaRequest("budget", testRejectedAt, "token-a", ingestquota.Limits{MaxEventsPerSecond: 1})
			if _, err := sequencer.Reject(ctx, initial); err != nil {
				t.Fatal(err)
			}
			before := readRejectionQuota(t, database, "token-a")
			accepted := reserveRequest("winner", "attempt")
			first, err := sequencer.Reserve(ctx, accepted)
			if err != nil {
				t.Fatal(err)
			}
			if committed {
				markAndCommit(t, sequencer, first.Sequence, accepted.AttemptID, testCommittedAt)
			}
			rejected := rejectionQuotaRequest("winner", testRejectedAt, "token-a", initial.RejectionAdmission.TokenLimits)
			got, err := sequencer.Reject(ctx, rejected)
			if err != nil || got.Rejected || got.AlreadyCommitted != committed || got.Sequence != first.Sequence {
				t.Fatalf("winning acceptance = %+v, %v", got, err)
			}
			if after := readRejectionQuota(t, database, "token-a"); after != before {
				t.Fatal("winning acceptance charged rejection quota")
			}
		})
	}
}

func isRejectionQuotaExceeded(err error) bool {
	var exceeded *ingestquota.ExceededError
	return errors.As(err, &exceeded)
}

func readRejectionQuota(t *testing.T, database *control.DB, token string) ingestquota.State {
	t.Helper()
	var state ingestquota.State
	if err := database.SQLDB().QueryRowContext(context.Background(), `
		SELECT max_rejections_per_second, max_metadata_bytes_per_second,
		       next_rejection_unix_nano, next_metadata_unix_nano, updated_at_unix_micro
		FROM ingest_rejection_buckets WHERE tenant_id = 'tenant-a' AND token_id = ?`, token,
	).Scan(&state.Limits.MaxEventsPerSecond, &state.Limits.MaxUncompressedBytesPerSecond,
		&state.NextEventAdmissionUnixNano, &state.NextByteAdmissionUnixNano, &state.UpdatedAtUnixMicro); err != nil {
		t.Fatal(err)
	}
	return state
}
