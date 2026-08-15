package visibility

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/ingestquota"
)

func TestSQLiteSequencerQuotaDenialRollsBackFreshReservation(t *testing.T) {
	t.Parallel()

	sequencer, database := openTestSequencer(t)
	ctx := context.Background()
	now := time.Date(2026, 8, 1, 12, 0, 0, 123_456_789, time.UTC)
	token := ingestquota.ScopeKey{
		Kind: ingestquota.ScopeKindToken, TenantID: "tenant-a", Identity: "token-a",
	}
	seedQuotaToken(t, database, token.Identity)
	limits := ingestquota.Limits{MaxEventsPerSecond: 1}
	blockedUntil := now.Add(5 * time.Second)
	seedQuotaBucket(t, database, token, limits, blockedUntil.UnixNano(), 0, now.Add(-time.Second))

	request := quotaReserveRequest("quota-denied", "attempt-one", now, token, limits, ingestquota.Limits{})
	if _, err := sequencer.Reserve(ctx, request); err == nil {
		t.Fatal("Reserve succeeded above quota")
	} else {
		var exceeded *ingestquota.ExceededError
		if !errors.As(err, &exceeded) {
			t.Fatalf("Reserve error = %v, want ExceededError", err)
		}
		if exceeded.Scope != token || exceeded.RetryAfter != 5*time.Second {
			t.Fatalf("quota error = %+v, want token / 5s", exceeded)
		}
	}
	assertQuotaObjectCounts(t, database, 0, 0, 0)
	if next := quotaNextEvent(t, database, token); next != blockedUntil.UnixNano() {
		t.Fatalf("denied token schedule = %d, want unchanged %d", next, blockedUntil.UnixNano())
	}

	request.AttemptID = "attempt-two"
	request.QuotaEvaluatedAt = blockedUntil
	reservation, err := sequencer.Reserve(ctx, request)
	if err != nil {
		t.Fatalf("Reserve at exact quota boundary: %v", err)
	}
	if reservation.Sequence != 1 {
		t.Fatalf("first admitted sequence = %d, want 1", reservation.Sequence)
	}
	assertQuotaObjectCounts(t, database, 1, 1, 1)
}

func TestSQLiteSequencerQuotaMixedScopesAreAtomic(t *testing.T) {
	t.Parallel()

	sequencer, database := openTestSequencer(t)
	now := time.Date(2026, 8, 1, 12, 1, 0, 0, time.UTC)
	limits := ingestquota.Limits{MaxEventsPerSecond: 10}
	token := ingestquota.ScopeKey{
		Kind: ingestquota.ScopeKindToken, TenantID: "tenant-a", Identity: "token-a",
	}
	seedQuotaToken(t, database, token.Identity)
	index := ingestquota.ScopeKey{
		Kind: ingestquota.ScopeKindIndex, TenantID: "tenant-a", Identity: "main",
	}
	seedQuotaBucket(t, database, token, limits, now.UnixNano(), 0, now.Add(-time.Second))
	seedQuotaBucket(t, database, index, limits, now.Add(3*time.Second).UnixNano(), 0, now.Add(-time.Second))

	request := quotaReserveRequest("quota-mixed", "attempt", now, token, limits, limits)
	_, err := sequencer.Reserve(context.Background(), request)
	var exceeded *ingestquota.ExceededError
	if !errors.As(err, &exceeded) || exceeded.Scope != index || exceeded.RetryAfter != 3*time.Second {
		t.Fatalf("Reserve error = %v (%+v), want blocked main index for 3s", err, exceeded)
	}
	if next := quotaNextEvent(t, database, token); next != now.UnixNano() {
		t.Fatalf("eligible token schedule changed on mixed denial: %d", next)
	}
	if next := quotaNextEvent(t, database, index); next != now.Add(3*time.Second).UnixNano() {
		t.Fatalf("blocked index schedule changed on mixed denial: %d", next)
	}
	assertQuotaObjectCounts(t, database, 0, 0, 0)
}

func TestSQLiteSequencerQuotaAdmissionSurvivesAbandonRestartAndPolicyChange(t *testing.T) {
	sequencer, database := openTestSequencer(t)
	ctx := context.Background()
	now := time.Date(2026, 8, 1, 12, 2, 0, 0, time.UTC)
	token := ingestquota.ScopeKey{
		Kind: ingestquota.ScopeKindToken, TenantID: "tenant-a", Identity: "token-a",
	}
	seedQuotaToken(t, database, token.Identity)
	limits := ingestquota.Limits{
		MaxEventsPerSecond: 10, MaxUncompressedBytesPerSecond: 100,
	}
	request := quotaReserveRequest("quota-replay", "attempt-one", now, token, limits, limits)
	first, err := sequencer.Reserve(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if err := sequencer.Abandon(ctx, first.Sequence, request.AttemptID); err != nil {
		t.Fatalf("Abandon: %v", err)
	}
	before := readQuotaBuckets(t, database)
	if len(before) != 2 {
		t.Fatalf("quota bucket count = %d, want token and index", len(before))
	}
	assertQuotaObjectCounts(t, database, 1, 1, 1)

	if err := sequencer.Close(); err != nil {
		t.Fatalf("Close original sequencer: %v", err)
	}
	reopened, err := NewSQLite(ctx, database)
	if err != nil {
		t.Fatalf("reopen sequencer: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })

	request.AttemptID = "attempt-two"
	request.QuotaAdmission = quotaAdmission(
		token,
		ingestquota.Limits{MaxEventsPerSecond: 1, MaxUncompressedBytesPerSecond: 1},
		"main",
		ingestquota.Limits{MaxEventsPerSecond: 1, MaxUncompressedBytesPerSecond: 1},
	)
	second, err := reopened.Reserve(ctx, request)
	if err != nil {
		t.Fatalf("fresh replacement reservation after restart: %v", err)
	}
	if second.Sequence != first.Sequence+1 {
		t.Fatalf("replacement sequence = %d, want %d", second.Sequence, first.Sequence+1)
	}
	if after := readQuotaBuckets(t, database); !equalQuotaBuckets(after, before) {
		t.Fatalf("durable replay recharged or rewrote quota: before=%+v after=%+v", before, after)
	}
	assertQuotaObjectCounts(t, database, 1, 2, 1)

	duplicate := request
	duplicate.AttemptID = "attempt-three"
	if _, err := reopened.Reserve(ctx, duplicate); !errors.Is(err, ErrAttemptInProgress) {
		t.Fatalf("concurrent duplicate error = %v, want ErrAttemptInProgress", err)
	}
	if after := readQuotaBuckets(t, database); !equalQuotaBuckets(after, before) {
		t.Fatalf("duplicate recharged quota: before=%+v after=%+v", before, after)
	}

	if err := reopened.Release(ctx, second.Sequence, request.AttemptID); err != nil {
		t.Fatalf("Release replacement: %v", err)
	}
	existingOnly := duplicate
	existingOnly.ExistingOnly = true
	existingOnly.QuotaAdmission = &ingestquota.Admission{}
	existingOnly.QuotaEvaluatedAt = time.Time{}
	if _, err := reopened.Reserve(ctx, existingOnly); err != nil {
		t.Fatalf("existing-only replay validated mutable quota: %v", err)
	}
}

func TestSQLiteSequencerQuotaPolicyChangeResetsDurableSchedule(t *testing.T) {
	t.Parallel()

	sequencer, database := openTestSequencer(t)
	ctx := context.Background()
	now := time.Date(2026, 8, 1, 12, 3, 0, 0, time.UTC)
	token := ingestquota.ScopeKey{
		Kind: ingestquota.ScopeKindToken, TenantID: "tenant-a", Identity: "token-a",
	}
	seedQuotaToken(t, database, token.Identity)
	limited := ingestquota.Limits{MaxEventsPerSecond: 10}
	first := quotaReserveRequest("quota-policy-one", "attempt-one", now, token, limited, limited)
	if _, err := sequencer.Reserve(ctx, first); err != nil {
		t.Fatal(err)
	}

	unlimited := quotaReserveRequest("quota-policy-two", "attempt-two", now, token, ingestquota.Limits{}, ingestquota.Limits{})
	if _, err := sequencer.Reserve(ctx, unlimited); err != nil {
		t.Fatalf("change to unlimited policy: %v", err)
	}
	for _, bucket := range readQuotaBuckets(t, database) {
		if bucket.eventRate != 0 || bucket.nextEvent != 0 {
			t.Fatalf("unlimited policy did not clear schedule: %+v", bucket)
		}
	}

	restored := quotaReserveRequest("quota-policy-three", "attempt-three", now, token, limited, limited)
	if _, err := sequencer.Reserve(ctx, restored); err != nil {
		t.Fatalf("restored rate inherited stale debt: %v", err)
	}
	if next := quotaNextEvent(t, database, token); next != now.Add(100*time.Millisecond).UnixNano() {
		t.Fatalf("restored token schedule = %d, want reset schedule", next)
	}
}

func TestSQLiteSequencerQuotaMaximumScopesBulkHydrationAndPersistence(t *testing.T) {
	t.Parallel()

	sequencer, database := openTestSequencer(t)
	ctx := context.Background()
	now := time.Date(2026, 8, 1, 12, 3, 30, 0, time.UTC)
	token := ingestquota.ScopeKey{
		Kind: ingestquota.ScopeKindToken, TenantID: "tenant-a", Identity: "token-a",
	}
	seedQuotaToken(t, database, token.Identity)
	limits := ingestquota.Limits{
		MaxEventsPerSecond: ingestquota.HardMaxEventsPerSecond,
	}
	charges := make([]ingestquota.Charge, 0, maximumQuotaAdmissionScopes)
	charges = append(charges, ingestquota.Charge{
		Scope: token, Limits: limits,
		Events:            ingestquota.HardMaxAdmissionEvents,
		UncompressedBytes: ingestquota.HardMaxAdmissionEvents,
	})
	lastIndex := ""
	for index := range int(ingestquota.HardMaxAdmissionEvents) {
		lastIndex = fmt.Sprintf("bulk-index-%04d", index)
		charges = append(charges, ingestquota.Charge{
			Scope: ingestquota.ScopeKey{
				Kind:     ingestquota.ScopeKindIndex,
				TenantID: token.TenantID,
				Identity: lastIndex,
			},
			Limits: limits, Events: 1, UncompressedBytes: 1,
		})
	}
	if len(charges) != maximumQuotaAdmissionScopes {
		t.Fatalf("quota charge count = %d, want %d", len(charges), maximumQuotaAdmissionScopes)
	}
	admission := &ingestquota.Admission{Charges: charges}
	first := reserveRequest("quota-bulk-first", "attempt-bulk-first")
	first.QuotaAdmission = admission
	first.QuotaEvaluatedAt = now
	if _, err := sequencer.Reserve(ctx, first); err != nil {
		t.Fatalf("Reserve maximum-scope admission: %v", err)
	}

	var bucketCount int
	if err := database.SQLDB().QueryRowContext(ctx, `
		SELECT count(*)
		FROM ingest_quota_buckets
		WHERE tenant_id = ?`, token.TenantID).Scan(&bucketCount); err != nil {
		t.Fatalf("count maximum-scope quota buckets: %v", err)
	}
	if bucketCount != maximumQuotaAdmissionScopes {
		t.Fatalf("maximum-scope quota buckets = %d, want %d", bucketCount, maximumQuotaAdmissionScopes)
	}

	blockedUntil := now.Add(2 * time.Second)
	if _, err := database.SQLDB().ExecContext(ctx, `
		UPDATE ingest_quota_buckets
		SET next_event_admission_unix_nano = ?
		WHERE tenant_id = ? AND scope_kind = 'index' AND scope_id = ?`,
		blockedUntil.UnixNano(), token.TenantID, lastIndex,
	); err != nil {
		t.Fatalf("extend final bulk quota bucket: %v", err)
	}
	second := reserveRequest("quota-bulk-second", "attempt-bulk-second")
	second.QuotaAdmission = admission
	second.QuotaEvaluatedAt = now.Add(time.Millisecond)
	_, err := sequencer.Reserve(ctx, second)
	var exceeded *ingestquota.ExceededError
	if !errors.As(err, &exceeded) {
		t.Fatalf("maximum-scope hydrated Reserve error = %v, want ExceededError", err)
	}
	wantScope := ingestquota.ScopeKey{
		Kind: ingestquota.ScopeKindIndex, TenantID: token.TenantID, Identity: lastIndex,
	}
	if exceeded.Scope != wantScope || exceeded.RetryAfter != blockedUntil.Sub(second.QuotaEvaluatedAt) {
		t.Fatalf("maximum-scope hydrated denial = %+v", exceeded)
	}
}

func TestSQLiteSequencerConcurrentExactQuotaAdmissionChargesOnce(t *testing.T) {
	t.Parallel()

	sequencer, database := openTestSequencer(t)
	now := time.Date(2026, 8, 1, 12, 4, 0, 0, time.UTC)
	token := ingestquota.ScopeKey{
		Kind: ingestquota.ScopeKindToken, TenantID: "tenant-a", Identity: "token-a",
	}
	seedQuotaToken(t, database, token.Identity)
	limits := ingestquota.Limits{MaxEventsPerSecond: 1}
	first := quotaReserveRequest("quota-concurrent", "attempt-one", now, token, limits, limits)
	second := first
	second.AttemptID = "attempt-two"

	start := make(chan struct{})
	results := make(chan error, 2)
	var wait sync.WaitGroup
	for _, request := range []ReserveRequest{first, second} {
		wait.Add(1)
		go func(request ReserveRequest) {
			defer wait.Done()
			<-start
			_, err := sequencer.Reserve(context.Background(), request)
			results <- err
		}(request)
	}
	close(start)
	wait.Wait()
	close(results)
	var admitted, inProgress int
	for err := range results {
		switch {
		case err == nil:
			admitted++
		case errors.Is(err, ErrAttemptInProgress):
			inProgress++
		default:
			t.Fatalf("concurrent Reserve error = %v", err)
		}
	}
	if admitted != 1 || inProgress != 1 {
		t.Fatalf("concurrent results admitted=%d in-progress=%d, want 1/1", admitted, inProgress)
	}
	assertQuotaObjectCounts(t, database, 1, 1, 1)
	if next := quotaNextEvent(t, database, token); next != now.Add(time.Second).UnixNano() {
		t.Fatalf("concurrent token schedule = %d, want one charge", next)
	}
}

func TestSQLiteSequencerConcurrentDistinctBatchesCompeteAtomically(t *testing.T) {
	t.Parallel()

	sequencer, database := openTestSequencer(t)
	now := time.Date(2026, 8, 1, 12, 5, 0, 0, time.UTC)
	token := ingestquota.ScopeKey{
		Kind: ingestquota.ScopeKindToken, TenantID: "tenant-a", Identity: "token-a",
	}
	seedQuotaToken(t, database, token.Identity)
	limits := ingestquota.Limits{MaxEventsPerSecond: 1}
	requests := []ReserveRequest{
		quotaReserveRequest("quota-distinct-a", "attempt-a", now, token, limits, limits),
		quotaReserveRequest("quota-distinct-b", "attempt-b", now, token, limits, limits),
	}

	start := make(chan struct{})
	results := make(chan error, len(requests))
	var wait sync.WaitGroup
	for _, request := range requests {
		wait.Add(1)
		go func(request ReserveRequest) {
			defer wait.Done()
			<-start
			_, err := sequencer.Reserve(context.Background(), request)
			results <- err
		}(request)
	}
	close(start)
	wait.Wait()
	close(results)

	var admitted, limited int
	for err := range results {
		if err == nil {
			admitted++
			continue
		}
		var exceeded *ingestquota.ExceededError
		if !errors.As(err, &exceeded) || exceeded.Scope != token ||
			exceeded.RetryAfter != time.Second {
			t.Fatalf("concurrent distinct Reserve error = %v", err)
		}
		limited++
	}
	if admitted != 1 || limited != 1 {
		t.Fatalf("concurrent distinct results admitted=%d limited=%d, want 1/1", admitted, limited)
	}
	assertQuotaObjectCounts(t, database, 1, 1, 1)
	if next := quotaNextEvent(t, database, token); next != now.Add(time.Second).UnixNano() {
		t.Fatalf("concurrent token schedule = %d, want one charge", next)
	}
}

func TestSQLiteSequencerRejectsMalformedQuotaAdmissionBeforeReservation(t *testing.T) {
	t.Parallel()

	sequencer, database := openTestSequencer(t)
	now := time.Date(2026, 8, 1, 12, 6, 0, 0, time.UTC)
	token := ingestquota.ScopeKey{
		Kind: ingestquota.ScopeKindToken, TenantID: "tenant-a", Identity: "token-a",
	}
	index := ingestquota.ScopeKey{
		Kind: ingestquota.ScopeKindIndex, TenantID: "tenant-a", Identity: "main",
	}
	validToken := ingestquota.Charge{
		Scope: token, Events: 1, UncompressedBytes: 100,
	}
	validIndex := ingestquota.Charge{
		Scope: index, Events: 1, UncompressedBytes: 100,
	}
	tests := []struct {
		name      string
		evaluated time.Time
		charges   []ingestquota.Charge
	}{
		{name: "missing index", evaluated: now, charges: []ingestquota.Charge{validToken}},
		{name: "missing token", evaluated: now, charges: []ingestquota.Charge{validIndex}},
		{
			name: "multiple tokens", evaluated: now,
			charges: []ingestquota.Charge{validToken, {
				Scope: ingestquota.ScopeKey{
					Kind: ingestquota.ScopeKindToken, TenantID: "tenant-a", Identity: "token-b",
				},
				Events: 1, UncompressedBytes: 100,
			}, validIndex},
		},
		{
			name: "multiple tenants", evaluated: now,
			charges: []ingestquota.Charge{validToken, {
				Scope: ingestquota.ScopeKey{
					Kind: ingestquota.ScopeKindIndex, TenantID: "tenant-b", Identity: "main",
				},
				Events: 1, UncompressedBytes: 100,
			}},
		},
		{
			name: "mismatched totals", evaluated: now,
			charges: []ingestquota.Charge{validToken, {
				Scope: index, Events: 1, UncompressedBytes: 99,
			}},
		},
		{
			name: "caller supplied state", evaluated: now,
			charges: []ingestquota.Charge{{
				Scope: token, Events: 1, UncompressedBytes: 100,
				State: &ingestquota.State{UpdatedAtUnixMicro: 1},
			}, validIndex},
		},
		{name: "missing evaluation time", charges: []ingestquota.Charge{validToken, validIndex}},
	}
	for index, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := reserveRequest("quota-malformed-"+test.name, "attempt-malformed-"+test.name)
			request.QuotaAdmission = &ingestquota.Admission{Charges: test.charges}
			request.QuotaEvaluatedAt = test.evaluated
			if _, err := sequencer.Reserve(context.Background(), request); err == nil ||
				!errors.Is(err, ErrInvalidArgument) {
				t.Fatalf("Reserve error = %v, want ErrInvalidArgument", err)
			}
			assertQuotaObjectCounts(t, database, 0, 0, 0)
			var bucketCount int
			if err := database.SQLDB().QueryRowContext(
				context.Background(),
				"SELECT count(*) FROM ingest_quota_buckets",
			).Scan(&bucketCount); err != nil {
				t.Fatal(err)
			}
			if bucketCount != 0 {
				t.Fatalf("malformed admission %d persisted %d buckets", index, bucketCount)
			}
		})
	}
}

func quotaReserveRequest(
	key string,
	attemptID string,
	evaluatedAt time.Time,
	token ingestquota.ScopeKey,
	tokenLimits ingestquota.Limits,
	indexLimits ingestquota.Limits,
) ReserveRequest {
	request := reserveRequest(key, attemptID)
	request.QuotaAdmission = quotaAdmission(token, tokenLimits, "main", indexLimits)
	request.QuotaEvaluatedAt = evaluatedAt
	return request
}

func quotaAdmission(
	token ingestquota.ScopeKey,
	tokenLimits ingestquota.Limits,
	index string,
	indexLimits ingestquota.Limits,
) *ingestquota.Admission {
	return &ingestquota.Admission{Charges: []ingestquota.Charge{
		{
			Scope: token, Limits: tokenLimits,
			Events: 1, UncompressedBytes: 100,
		},
		{
			Scope: ingestquota.ScopeKey{
				Kind: ingestquota.ScopeKindIndex, TenantID: token.TenantID, Identity: index,
			},
			Limits: indexLimits, Events: 1, UncompressedBytes: 100,
		},
	}}
}

func seedQuotaToken(t *testing.T, database *control.DB, tokenID string) {
	t.Helper()
	if _, err := database.SQLDB().ExecContext(context.Background(), `
		INSERT INTO ingestion_tokens (
			ingestion_token_id, version, name, token_prefix, token_digest,
			state, created_at_unix_micro, updated_at_unix_micro,
			bound_collector_id
		) VALUES (?, 1, 'quota token', 'quota123', zeroblob(32),
			'active', 1, 1, 'collector-a')`, tokenID,
	); err != nil {
		t.Fatalf("seed quota token: %v", err)
	}
}

func seedQuotaBucket(
	t *testing.T,
	database *control.DB,
	scope ingestquota.ScopeKey,
	limits ingestquota.Limits,
	nextEvent int64,
	nextByte int64,
	updatedAt time.Time,
) {
	t.Helper()
	var tokenOwnerID any
	if scope.Kind == ingestquota.ScopeKindToken {
		tokenOwnerID = scope.Identity
	}
	if _, err := database.SQLDB().ExecContext(context.Background(), `
		INSERT INTO ingest_quota_buckets (
			tenant_id, scope_kind, scope_id,
			max_ingest_events_per_second,
			max_ingest_uncompressed_bytes_per_second,
			next_event_admission_unix_nano,
			next_byte_admission_unix_nano,
			updated_at_unix_micro,
			token_owner_id
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		scope.TenantID,
		string(scope.Kind),
		scope.Identity,
		limits.MaxEventsPerSecond,
		limits.MaxUncompressedBytesPerSecond,
		nextEvent,
		nextByte,
		updatedAt.UnixMicro(),
		tokenOwnerID,
	); err != nil {
		t.Fatalf("seed quota bucket: %v", err)
	}
}

func assertQuotaObjectCounts(
	t *testing.T,
	database *control.DB,
	wantIdentities int,
	wantReservations int,
	wantAdmissions int,
) {
	t.Helper()
	for table, want := range map[string]int{
		"ingest_batch_identities":        wantIdentities,
		"ingest_visibility_reservations": wantReservations,
		"ingest_quota_admissions":        wantAdmissions,
	} {
		var count int
		if err := database.SQLDB().QueryRowContext(
			context.Background(),
			"SELECT count(*) FROM "+table,
		).Scan(&count); err != nil {
			t.Fatalf("count %s: %v", table, err)
		}
		if count != want {
			t.Fatalf("%s count = %d, want %d", table, count, want)
		}
	}
}

func quotaNextEvent(t *testing.T, database *control.DB, scope ingestquota.ScopeKey) int64 {
	t.Helper()
	var next int64
	if err := database.SQLDB().QueryRowContext(context.Background(), `
		SELECT next_event_admission_unix_nano
		FROM ingest_quota_buckets
		WHERE tenant_id = ? AND scope_kind = ? AND scope_id = ?`,
		scope.TenantID, string(scope.Kind), scope.Identity,
	).Scan(&next); err != nil {
		t.Fatalf("read quota next event: %v", err)
	}
	return next
}

type storedQuotaBucket struct {
	tenantID  string
	kind      string
	identity  string
	eventRate int64
	byteRate  int64
	nextEvent int64
	nextByte  int64
	updatedAt int64
}

func readQuotaBuckets(t *testing.T, database *control.DB) []storedQuotaBucket {
	t.Helper()
	rows, err := database.SQLDB().QueryContext(context.Background(), `
		SELECT tenant_id, scope_kind, scope_id,
		       max_ingest_events_per_second,
		       max_ingest_uncompressed_bytes_per_second,
		       next_event_admission_unix_nano,
		       next_byte_admission_unix_nano,
		       updated_at_unix_micro
		FROM ingest_quota_buckets
		ORDER BY tenant_id, scope_kind, scope_id`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var buckets []storedQuotaBucket
	for rows.Next() {
		var bucket storedQuotaBucket
		if err := rows.Scan(
			&bucket.tenantID,
			&bucket.kind,
			&bucket.identity,
			&bucket.eventRate,
			&bucket.byteRate,
			&bucket.nextEvent,
			&bucket.nextByte,
			&bucket.updatedAt,
		); err != nil {
			t.Fatal(err)
		}
		buckets = append(buckets, bucket)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return buckets
}

func equalQuotaBuckets(left, right []storedQuotaBucket) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}
