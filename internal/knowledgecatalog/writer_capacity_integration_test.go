package knowledgecatalog_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/knowledgecatalog"
	"google.golang.org/protobuf/proto"
)

const (
	writerNormalIdempotencyCapacity   = 16_384
	writerAbsoluteIdempotencyCapacity = 20_480
	writerProtectiveReceiptReserve    = writerAbsoluteIdempotencyCapacity - writerNormalIdempotencyCapacity
	writerMaximumIdempotencyReclaim   = writerProtectiveReceiptReserve + 1
)

func TestWriterReplayPrecedesNormalAndAbsoluteIdempotencyCapacity(t *testing.T) {
	harness := newWriterBlackboxHarness(t)
	now := time.Now().UTC().Truncate(time.Microsecond)
	writer, calls := newCapacityTestWriter(
		t,
		harness,
		now,
		365*24*time.Hour,
		"ko_capacity_replay",
	)
	request := capacityCreateRequest("capacity-replay-anchor", "capacity-replay-anchor-0001")
	committed, err := writer.Create(harness.actorCtx, harness.writeScope, request)
	if err != nil {
		t.Fatalf("create replay anchor: %v", err)
	}

	seedCapacityReceiptCopies(t, harness.database, capacityReceiptSeed{
		SourceRequestID: request.GetClientRequestId(),
		RequestIDPrefix: "normal-capacity-filler-",
		Count:           writerNormalIdempotencyCapacity - 1,
		Retention:       365 * 24 * time.Hour,
	})
	normalSnapshot := readWriterAuthoritySnapshot(t, harness.database)
	if normalSnapshot.IdempotencyCount != writerNormalIdempotencyCapacity ||
		normalSnapshot.TableCounts["knowledge_mutation_idempotency"] != writerNormalIdempotencyCapacity {
		t.Fatalf("normal-capacity authority = %#v", normalSnapshot)
	}

	replayed, err := writer.Create(
		harness.actorCtx,
		harness.writeScope,
		proto.Clone(request).(*opensplunkv1.CreateKnowledgeObjectRequest),
	)
	if err != nil || !proto.Equal(replayed, committed) {
		t.Fatalf("exact replay at normal capacity = (%v, %v), want %v", replayed, err, committed)
	}
	if calls.IDs.Load() != 1 || calls.Clocks.Load() != 1 {
		t.Fatalf("generator calls after normal-capacity replay = IDs %d clocks %d, want 1/1", calls.IDs.Load(), calls.Clocks.Load())
	}
	assertWriterAuthoritySnapshotsEqual(t, readWriterAuthoritySnapshot(t, harness.database), normalSnapshot)

	assertCapacityRejectedWithoutPublication(
		t,
		harness,
		writer,
		capacityCreateRequest("normal-capacity-rejected", "normal-capacity-reject-0001"),
		normalSnapshot,
	)

	// The schema reserves rows 16,385 through 20,480 for quarantine. This
	// fixture is testing replay admission, not quarantine publication, so it
	// stages unrelated synthetic receipts with matching immutable commit
	// authorities. The final physical count and ledger agree exactly at the
	// absolute structural ceiling.
	seedCapacityReceiptCopies(t, harness.database, capacityReceiptSeed{
		SourceRequestID: request.GetClientRequestId(),
		RequestIDPrefix: "absolute-capacity-filler-",
		Count:           writerAbsoluteIdempotencyCapacity - writerNormalIdempotencyCapacity,
		Retention:       365 * 24 * time.Hour,
	})
	absoluteSnapshot := readWriterAuthoritySnapshot(t, harness.database)
	if absoluteSnapshot.IdempotencyCount != writerAbsoluteIdempotencyCapacity ||
		absoluteSnapshot.TableCounts["knowledge_mutation_idempotency"] != writerAbsoluteIdempotencyCapacity {
		t.Fatalf("absolute-capacity authority = %#v", absoluteSnapshot)
	}

	replayed, err = writer.Create(
		harness.actorCtx,
		harness.writeScope,
		proto.Clone(request).(*opensplunkv1.CreateKnowledgeObjectRequest),
	)
	if err != nil || !proto.Equal(replayed, committed) {
		t.Fatalf("exact replay at absolute capacity = (%v, %v), want %v", replayed, err, committed)
	}
	if calls.IDs.Load() != 1 || calls.Clocks.Load() != 1 {
		t.Fatalf("generator calls after absolute-capacity replay = IDs %d clocks %d, want 1/1", calls.IDs.Load(), calls.Clocks.Load())
	}
	assertWriterAuthoritySnapshotsEqual(t, readWriterAuthoritySnapshot(t, harness.database), absoluteSnapshot)
	assertWriterCatalogIntegrity(t, harness.database)
}

func TestWriterUnexpiredIdempotencyCapacityRejectsBeforeEveryPublicationAuthority(t *testing.T) {
	harness := newWriterBlackboxHarness(t)
	now := time.Now().UTC().Truncate(time.Microsecond)
	writer, calls := newCapacityTestWriter(
		t,
		harness,
		now,
		365*24*time.Hour,
		"ko_capacity_unexpired",
	)
	request := capacityCreateRequest("unexpired-capacity-anchor", "unexpired-capacity-anchor-01")
	if _, err := writer.Create(harness.actorCtx, harness.writeScope, request); err != nil {
		t.Fatalf("create unexpired capacity anchor: %v", err)
	}
	seedCapacityReceiptCopies(t, harness.database, capacityReceiptSeed{
		SourceRequestID: request.GetClientRequestId(),
		RequestIDPrefix: "unexpired-capacity-filler-",
		Count:           writerNormalIdempotencyCapacity - 1,
		Retention:       365 * 24 * time.Hour,
	})

	stable := readWriterAuthoritySnapshot(t, harness.database)
	assertCapacityRejectedWithoutPublication(
		t,
		harness,
		writer,
		capacityCreateRequest("unexpired-capacity-rejected", "unexpired-capacity-reject-01"),
		stable,
	)
	if calls.IDs.Load() != 1 || calls.Clocks.Load() != 1 {
		t.Fatalf("generator calls after capacity rejection = IDs %d clocks %d, want 1/1", calls.IDs.Load(), calls.Clocks.Load())
	}
	assertWriterCatalogIntegrity(t, harness.database)
}

func TestWriterPressureReclaimsOnlyABoundedOldestExpiredPrefix(t *testing.T) {
	harness := newWriterBlackboxHarness(t)
	wallNow := time.Now().UTC().Truncate(time.Microsecond)
	oldTime := wallNow.Add(-366 * 24 * time.Hour)
	oldWriter, _ := newCapacityTestWriter(
		t,
		harness,
		oldTime,
		365*24*time.Hour,
		"ko_capacity_old",
	)
	oldRequest := capacityCreateRequest("expired-capacity-anchor", "expired-capacity-anchor-0001")
	if _, err := oldWriter.Create(harness.actorCtx, harness.writeScope, oldRequest); err != nil {
		t.Fatalf("create expired capacity anchor: %v", err)
	}

	// Increasing the retention fence by one microsecond per row establishes a
	// total, deterministic oldest-first order without relying on insertion order.
	const expiredFillers = writerProtectiveReceiptReserve + 1
	seedCapacityReceiptCopies(t, harness.database, capacityReceiptSeed{
		SourceRequestID: oldRequest.GetClientRequestId(),
		RequestIDPrefix: "expired-capacity-000-filler-",
		Count:           expiredFillers,
		Retention:       7 * 24 * time.Hour,
	})

	currentWriter, _ := newCapacityTestWriter(
		t,
		harness,
		wallNow,
		365*24*time.Hour,
		"ko_capacity_current",
	)
	currentRequest := capacityCreateRequest("current-capacity-anchor", "current-capacity-anchor-0001")
	if _, err := currentWriter.Create(harness.actorCtx, harness.writeScope, currentRequest); err != nil {
		t.Fatalf("create current capacity anchor: %v", err)
	}
	seedCapacityReceiptCopies(t, harness.database, capacityReceiptSeed{
		SourceRequestID: currentRequest.GetClientRequestId(),
		RequestIDPrefix: "current-capacity-filler-",
		Count: writerNormalIdempotencyCapacity -
			(expiredFillers + 2), // both real anchor receipts also consume one row
		Retention: 365 * 24 * time.Hour,
	})
	before := readWriterAuthoritySnapshot(t, harness.database)
	if before.IdempotencyCount != writerNormalIdempotencyCapacity {
		t.Fatalf("mixed pressure fixture idempotency count = %d, want %d", before.IdempotencyCount, writerNormalIdempotencyCapacity)
	}

	// Cancel immediately after capacity reclamation, when the writer first asks
	// for a new identity. The next context-bound query must fail and roll the
	// reclaimed receipts back along with the otherwise empty publication.
	cancelContext, cancel := context.WithCancel(harness.actorCtx)
	var cancelIDCalls atomic.Int64
	var cancelClockCalls atomic.Int64
	cancelWriter, err := knowledgecatalog.NewWriter(harness.database, harness.audit, knowledgecatalog.WriterOptions{
		Clock: func() time.Time {
			cancelClockCalls.Add(1)
			return wallNow
		},
		IDGenerator: func() (string, error) {
			cancelIDCalls.Add(1)
			cancel()
			return "ko_capacity_canceled_000001", nil
		},
		IdempotencyRetention: 365 * 24 * time.Hour,
	})
	if err != nil {
		t.Fatalf("NewWriter(cancellation fixture): %v", err)
	}
	if _, err := cancelWriter.Create(
		cancelContext,
		harness.writeScope,
		capacityCreateRequest("capacity-reclaim-canceled", "capacity-reclaim-cancel-0001"),
	); !errors.Is(err, context.Canceled) {
		t.Fatalf("Create() canceled after reclamation error = %v, want context.Canceled", err)
	}
	if cancelIDCalls.Load() != 1 || cancelClockCalls.Load() != 0 {
		t.Fatalf("canceled reclaim generator calls = IDs %d clocks %d, want 1/0", cancelIDCalls.Load(), cancelClockCalls.Load())
	}
	assertWriterAuthoritySnapshotsEqual(t, readWriterAuthoritySnapshot(t, harness.database), before)

	newRequest := capacityCreateRequest("capacity-after-reclaim", "capacity-after-reclaim-0001")
	if _, err := currentWriter.Create(harness.actorCtx, harness.writeScope, newRequest); err != nil {
		t.Fatalf("create after expired-receipt reclamation: %v", err)
	}
	after := readWriterAuthoritySnapshot(t, harness.database)
	deleted := before.IdempotencyCount + 1 - after.IdempotencyCount
	if deleted != 1 {
		t.Fatalf("expired receipts reclaimed at normal capacity = %d, want exact-needed 1", deleted)
	}
	if after.CatalogRevision != before.CatalogRevision+1 ||
		after.VersionCount != before.VersionCount+1 ||
		after.IdentityCount != before.IdentityCount+1 ||
		after.AuditNextSequence != before.AuditNextSequence+1 ||
		after.AuditEventCount != before.AuditEventCount+1 {
		t.Fatalf("authority after bounded reclaim = %#v, before %#v", after, before)
	}
	assertExpiredCapacityPrefixReclaimed(t, harness.database, int(deleted), expiredFillers)
	assertReceiptPrefixCount(
		t,
		harness.database,
		"current-capacity-filler-",
		writerNormalIdempotencyCapacity-(expiredFillers+2),
	)
	assertCapacityReceiptExists(t, harness.database, oldRequest.GetClientRequestId())
	assertCapacityReceiptExists(t, harness.database, currentRequest.GetClientRequestId())
	assertCapacityReceiptExists(t, harness.database, newRequest.GetClientRequestId())
	assertWriterCatalogIntegrity(t, harness.database)
}

func TestWriterAbsoluteCapacityReclaimsExactNeededOldestReceiptsAndPreservesReserve(t *testing.T) {
	harness := newWriterBlackboxHarness(t)
	wallNow := time.Now().UTC().Truncate(time.Microsecond)
	oldWriter, _ := newCapacityTestWriter(
		t,
		harness,
		wallNow.Add(-366*24*time.Hour),
		365*24*time.Hour,
		"ko_absolute_old",
	)
	oldRequest := capacityCreateRequest("absolute-expired-anchor", "absolute-expired-anchor-0001")
	if _, err := oldWriter.Create(harness.actorCtx, harness.writeScope, oldRequest); err != nil {
		t.Fatalf("create absolute expired anchor: %v", err)
	}

	// Keep one ordered expired filler beyond the exact 4,097 rows needed to
	// cross from the 20,480 structural ceiling to one insertable normal slot.
	const expiredFillers = writerMaximumIdempotencyReclaim + 1
	seedCapacityReceiptCopies(t, harness.database, capacityReceiptSeed{
		SourceRequestID: oldRequest.GetClientRequestId(),
		RequestIDPrefix: "absolute-expired-000-filler-",
		Count:           expiredFillers,
		Retention:       7 * 24 * time.Hour,
	})

	currentWriter, _ := newCapacityTestWriter(
		t,
		harness,
		wallNow,
		365*24*time.Hour,
		"ko_absolute_current",
	)
	currentRequest := capacityCreateRequest("absolute-current-anchor", "absolute-current-anchor-0001")
	if _, err := currentWriter.Create(harness.actorCtx, harness.writeScope, currentRequest); err != nil {
		t.Fatalf("create absolute current anchor: %v", err)
	}
	const normalUnexpiredFillers = writerNormalIdempotencyCapacity - (expiredFillers + 2)
	seedCapacityReceiptCopies(t, harness.database, capacityReceiptSeed{
		SourceRequestID: currentRequest.GetClientRequestId(),
		RequestIDPrefix: "absolute-normal-unexpired-",
		Count:           normalUnexpiredFillers,
		Retention:       365 * 24 * time.Hour,
	})
	seedCapacityReceiptCopies(t, harness.database, capacityReceiptSeed{
		SourceRequestID: currentRequest.GetClientRequestId(),
		RequestIDPrefix: "absolute-reserve-unexpired-",
		Count:           writerProtectiveReceiptReserve,
		Retention:       365 * 24 * time.Hour,
	})
	before := readWriterAuthoritySnapshot(t, harness.database)
	if before.IdempotencyCount != writerAbsoluteIdempotencyCapacity ||
		before.TableCounts["knowledge_mutation_idempotency"] != writerAbsoluteIdempotencyCapacity {
		t.Fatalf("absolute reclaim fixture authority = %#v", before)
	}

	newRequest := capacityCreateRequest("absolute-capacity-after-reclaim", "absolute-capacity-reclaim-0001")
	if _, err := currentWriter.Create(harness.actorCtx, harness.writeScope, newRequest); err != nil {
		t.Fatalf("create after absolute-capacity receipt reclamation: %v", err)
	}
	after := readWriterAuthoritySnapshot(t, harness.database)
	deleted := before.IdempotencyCount + 1 - after.IdempotencyCount
	if deleted != writerMaximumIdempotencyReclaim {
		t.Fatalf("absolute-capacity receipts reclaimed = %d, want exact-needed %d", deleted, writerMaximumIdempotencyReclaim)
	}
	if after.IdempotencyCount != writerNormalIdempotencyCapacity ||
		after.CatalogRevision != before.CatalogRevision+1 ||
		after.VersionCount != before.VersionCount+1 ||
		after.IdentityCount != before.IdentityCount+1 ||
		after.AuditNextSequence != before.AuditNextSequence+1 ||
		after.AuditEventCount != before.AuditEventCount+1 {
		t.Fatalf("authority after absolute bounded reclaim = %#v, before %#v", after, before)
	}
	assertCapacityReceiptSuffixReclaimed(
		t,
		harness.database,
		"absolute-expired-000-filler-",
		writerMaximumIdempotencyReclaim,
		expiredFillers,
	)
	assertReceiptPrefixCount(t, harness.database, "absolute-normal-unexpired-", normalUnexpiredFillers)
	assertReceiptPrefixCount(t, harness.database, "absolute-reserve-unexpired-", writerProtectiveReceiptReserve)
	assertCapacityReceiptExists(t, harness.database, oldRequest.GetClientRequestId())
	assertCapacityReceiptExists(t, harness.database, currentRequest.GetClientRequestId())
	assertCapacityReceiptExists(t, harness.database, newRequest.GetClientRequestId())
	assertWriterCatalogIntegrity(t, harness.database)
}

func TestWriterIdempotencyReclaimUsesCoveringRetentionIndexWithoutTempSort(t *testing.T) {
	harness := newWriterBlackboxHarness(t)
	now := time.Now().UnixMicro()
	tests := []struct {
		name         string
		query        string
		wantCovering bool
	}{
		{
			name: "delete prefix",
			query: `DELETE FROM knowledge_mutation_idempotency
				WHERE (tenant_id, actor_kind, actor_id, route, client_request_id) IN (
					SELECT tenant_id, actor_kind, actor_id, route, client_request_id
					FROM knowledge_mutation_idempotency
					WHERE tenant_id = ? AND retain_until_unix_micro <= ?
					ORDER BY retain_until_unix_micro, created_at_unix_micro,
					         actor_kind, actor_id, route, client_request_id
					LIMIT ?
				)`,
			wantCovering: true,
		},
		{
			name: "width preflight",
			query: `SELECT
					length(CAST(tenant_id AS BLOB)),
					length(CAST(actor_kind AS BLOB)),
					length(CAST(actor_id AS BLOB)),
					length(CAST(route AS BLOB)),
					length(CAST(client_request_id AS BLOB)),
					length(CAST(mutation_kind AS BLOB)),
					length(request_digest), length(outcome_proto),
					length(committed_catalog_state_token),
					length(CAST(knowledge_object_id AS BLOB))
				FROM knowledge_mutation_idempotency
				INDEXED BY knowledge_mutation_idempotency_retention_idx
				WHERE tenant_id = ? AND retain_until_unix_micro <= ?
				ORDER BY retain_until_unix_micro, created_at_unix_micro,
				         actor_kind, actor_id, route, client_request_id
				LIMIT ?`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			plan := explainCapacityQueryPlan(
				t,
				harness.database.SQLDB(),
				test.query,
				writerTestTenant,
				now,
				writerMaximumIdempotencyReclaim,
			)
			if !strings.Contains(plan, "KNOWLEDGE_MUTATION_IDEMPOTENCY_RETENTION_IDX") {
				t.Fatalf("idempotency reclaim %s does not use its retention index:\n%s", test.name, plan)
			}
			if test.wantCovering && !strings.Contains(plan, "USING COVERING INDEX") {
				t.Fatalf("idempotency reclaim %s does not use a covering access path:\n%s", test.name, plan)
			}
			if strings.Contains(plan, "USE TEMP B-TREE") {
				t.Fatalf("idempotency reclaim %s performs a temporary sort:\n%s", test.name, plan)
			}
		})
	}
}

func TestWriterIdempotencyRetentionRoundsUpAndRejectsOneNanosecondOutside(t *testing.T) {
	harness := newWriterBlackboxHarness(t)
	now := time.Now().UTC().Truncate(time.Microsecond)
	tests := []struct {
		name          string
		retention     time.Duration
		wantRetention time.Duration
		objectID      string
		requestID     string
	}{
		{
			name:          "exact seven days",
			retention:     7 * 24 * time.Hour,
			wantRetention: 7 * 24 * time.Hour,
			objectID:      "ko_capacity_retention_min",
			requestID:     "retention-minimum-request-01",
		},
		{
			name:          "exact three hundred sixty five days",
			retention:     365 * 24 * time.Hour,
			wantRetention: 365 * 24 * time.Hour,
			objectID:      "ko_capacity_retention_max",
			requestID:     "retention-maximum-request-01",
		},
		{
			name:          "minimum plus one nanosecond rounds up",
			retention:     7*24*time.Hour + time.Nanosecond,
			wantRetention: 7*24*time.Hour + time.Microsecond,
			objectID:      "ko_capacity_retention_min_round",
			requestID:     "retention-minimum-round-request",
		},
		{
			name:          "maximum minus nine hundred ninety nine nanoseconds rounds to maximum",
			retention:     365*24*time.Hour - 999*time.Nanosecond,
			wantRetention: 365 * 24 * time.Hour,
			objectID:      "ko_capacity_retention_max_round",
			requestID:     "retention-maximum-round-request",
		},
	}
	for index, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			writer, _ := newCapacityTestWriter(
				t,
				harness,
				now.Add(time.Duration(index)*time.Microsecond),
				test.retention,
				test.objectID,
			)
			request := capacityCreateRequest("retention-boundary-"+fmt.Sprint(index), test.requestID)
			if _, err := writer.Create(harness.actorCtx, harness.writeScope, request); err != nil {
				t.Fatalf("Create() at exact retention boundary: %v", err)
			}
			if got := capacityReceiptRetention(t, harness.database, test.requestID); got != test.wantRetention {
				t.Fatalf("persisted retention = %s, want %s for configured %s", got, test.wantRetention, test.retention)
			}
		})
	}

	stable := readWriterAuthoritySnapshot(t, harness.database)
	for _, retention := range []time.Duration{
		7*24*time.Hour - time.Nanosecond,
		365*24*time.Hour + time.Nanosecond,
	} {
		if _, err := knowledgecatalog.NewWriter(harness.database, harness.audit, knowledgecatalog.WriterOptions{
			IdempotencyRetention: retention,
		}); !errors.Is(err, control.ErrInvalidArgument) {
			t.Fatalf("NewWriter(retention=%s) error = %v, want ErrInvalidArgument", retention, err)
		}
	}
	assertWriterAuthoritySnapshotsEqual(t, readWriterAuthoritySnapshot(t, harness.database), stable)
	assertWriterCatalogIntegrity(t, harness.database)
}

func TestWriterRetentionUsesDatabaseAnchorAndShortenedFenceCannotReexecute(t *testing.T) {
	harness := newWriterBlackboxHarness(t)
	wallNow := time.Now().UTC().Truncate(time.Microsecond)
	staleTime := wallNow.Add(-400 * 24 * time.Hour)
	staleWriter, _ := newCapacityTestWriter(
		t,
		harness,
		staleTime,
		365*24*time.Hour,
		"ko_retention_stale",
	)
	staleRequest := capacityCreateRequest("retention-stale-anchor", "retention-stale-anchor-0001")
	beforeDatabaseTime := time.Now().UTC().Add(-time.Second).UnixMicro()
	committed, err := staleWriter.Create(harness.actorCtx, harness.writeScope, staleRequest)
	if err != nil {
		t.Fatalf("create with stale configured clock: %v", err)
	}
	afterDatabaseTime := time.Now().UTC().Add(time.Second).UnixMicro()
	var occurredAt int64
	var retentionAnchor int64
	var retainUntil int64
	var encodedOutcome []byte
	if err := harness.database.SQLDB().QueryRowContext(t.Context(), `
		SELECT created_at_unix_micro, retention_anchor_unix_micro,
		       retain_until_unix_micro, outcome_proto
		FROM knowledge_mutation_idempotency
		WHERE tenant_id = ? AND route = 'objects.create'
		  AND client_request_id = ?`,
		writerTestTenant,
		staleRequest.GetClientRequestId(),
	).Scan(&occurredAt, &retentionAnchor, &retainUntil, &encodedOutcome); err != nil {
		t.Fatalf("read stale-clock receipt: %v", err)
	}
	if occurredAt != staleTime.UnixMicro() || retentionAnchor < beforeDatabaseTime ||
		retentionAnchor > afterDatabaseTime ||
		retainUntil-retentionAnchor != int64(365*24*time.Hour/time.Microsecond) {
		t.Fatalf("stale-clock retention = occurred %d anchor %d retain %d, database window [%d,%d]",
			occurredAt, retentionAnchor, retainUntil, beforeDatabaseTime, afterDatabaseTime)
	}
	envelope := &opensplunkv1.KnowledgeMutationOutcomeRecord{}
	if err := proto.Unmarshal(encodedOutcome, envelope); err != nil ||
		envelope.GetOccurredAtUnixMicro() != occurredAt ||
		envelope.GetRetentionAnchorUnixMicro() != retentionAnchor ||
		envelope.GetRetainUntilUnixMicro() != retainUntil {
		t.Fatalf("stale-clock outcome authority = (%v, %v)", envelope, err)
	}

	var receiptTriggerSQL string
	if err := harness.database.SQLDB().QueryRowContext(t.Context(), `
		SELECT sql FROM sqlite_schema
		WHERE type = 'trigger'
		  AND name = 'knowledge_mutation_idempotency_update_is_forbidden'`).Scan(&receiptTriggerSQL); err != nil {
		t.Fatalf("read receipt immutability trigger: %v", err)
	}
	if _, err := harness.database.SQLDB().ExecContext(t.Context(), `
		DROP TRIGGER knowledge_mutation_idempotency_update_is_forbidden`); err != nil {
		t.Fatalf("drop receipt immutability trigger: %v", err)
	}
	shortenedRetainUntil := occurredAt + int64(7*24*time.Hour/time.Microsecond)
	shortenedEnvelope := proto.Clone(envelope).(*opensplunkv1.KnowledgeMutationOutcomeRecord)
	shortenedEnvelope.RetentionAnchorUnixMicro = occurredAt
	shortenedEnvelope.RetainUntilUnixMicro = shortenedRetainUntil
	shortenedOutcome, err := (proto.MarshalOptions{Deterministic: true}).Marshal(shortenedEnvelope)
	if err != nil {
		t.Fatalf("encode shortened retained outcome: %v", err)
	}
	if _, err := harness.database.SQLDB().ExecContext(t.Context(), `
		UPDATE knowledge_mutation_idempotency
		SET outcome_proto = ?, retention_anchor_unix_micro = ?,
		    retain_until_unix_micro = ?
		WHERE tenant_id = ? AND route = 'objects.create'
		  AND client_request_id = ?`,
		shortenedOutcome,
		occurredAt,
		shortenedRetainUntil,
		writerTestTenant,
		staleRequest.GetClientRequestId(),
	); err != nil {
		t.Fatalf("shorten retained receipt fence: %v", err)
	}
	if _, err := harness.database.SQLDB().ExecContext(t.Context(), receiptTriggerSQL); err != nil {
		t.Fatalf("restore receipt immutability trigger: %v", err)
	}

	currentWriter, _ := newCapacityTestWriter(
		t,
		harness,
		wallNow,
		365*24*time.Hour,
		"ko_retention_current",
	)
	currentRequest := capacityCreateRequest("retention-current-anchor", "retention-current-anchor-0001")
	if _, err := currentWriter.Create(harness.actorCtx, harness.writeScope, currentRequest); err != nil {
		t.Fatalf("create current retention anchor: %v", err)
	}
	seedCapacityReceiptCopies(t, harness.database, capacityReceiptSeed{
		SourceRequestID: currentRequest.GetClientRequestId(),
		RequestIDPrefix: "retention-current-filler-",
		Count:           writerNormalIdempotencyCapacity - 2,
		Retention:       365 * 24 * time.Hour,
	})
	stable := readWriterAuthoritySnapshot(t, harness.database)
	probeWriter, calls := newCapacityTestWriter(
		t,
		harness,
		wallNow,
		365*24*time.Hour,
		"ko_retention_probe",
	)
	if response, err := probeWriter.Create(
		harness.actorCtx,
		harness.writeScope,
		capacityCreateRequest("retention-probe", "retention-probe-request-0001"),
	); response != nil || !errors.Is(err, knowledgecatalog.ErrCorrupt) {
		t.Fatalf("Create() with shortened retention authority = (%v, %v), want nil/ErrCorrupt", response, err)
	}
	if calls.IDs.Load() != 0 || calls.Clocks.Load() != 0 {
		t.Fatalf("shortened retention reached generators: IDs=%d clocks=%d", calls.IDs.Load(), calls.Clocks.Load())
	}
	assertWriterAuthoritySnapshotsEqual(t, readWriterAuthoritySnapshot(t, harness.database), stable)
	if replayed, err := staleWriter.Create(
		harness.actorCtx,
		harness.writeScope,
		proto.Clone(staleRequest).(*opensplunkv1.CreateKnowledgeObjectRequest),
	); replayed != nil || !errors.Is(err, knowledgecatalog.ErrCorrupt) {
		t.Fatalf("exact retry with shortened fence = (%v, %v), want nil/ErrCorrupt; original=%v", replayed, err, committed)
	}

	if _, err := harness.database.SQLDB().ExecContext(t.Context(), `
		DROP TRIGGER knowledge_mutation_idempotency_update_is_forbidden;
		UPDATE knowledge_mutation_idempotency
		SET outcome_proto = ?, retention_anchor_unix_micro = ?,
		    retain_until_unix_micro = ?
		WHERE tenant_id = ? AND route = 'objects.create'
		  AND client_request_id = ?`,
		encodedOutcome,
		retentionAnchor,
		retainUntil,
		writerTestTenant,
		staleRequest.GetClientRequestId(),
	); err != nil {
		t.Fatalf("restore retained receipt fence: %v", err)
	}
	if _, err := harness.database.SQLDB().ExecContext(t.Context(), receiptTriggerSQL); err != nil {
		t.Fatalf("restore final receipt immutability trigger: %v", err)
	}
	assertWriterCatalogIntegrity(t, harness.database)
}

func TestWriterIdempotencyLedgerMismatchFailsClosedWithoutMutation(t *testing.T) {
	harness := newWriterBlackboxHarness(t)
	now := time.Now().UTC().Truncate(time.Microsecond)
	writer, _ := newCapacityTestWriter(
		t,
		harness,
		now,
		365*24*time.Hour,
		"ko_capacity_corrupt",
	)
	anchor := capacityCreateRequest("ledger-mismatch-anchor", "ledger-mismatch-anchor-0001")
	if _, err := writer.Create(harness.actorCtx, harness.writeScope, anchor); err != nil {
		t.Fatalf("create ledger mismatch anchor: %v", err)
	}
	if _, err := harness.database.SQLDB().ExecContext(t.Context(), `
		UPDATE knowledge_catalog_tenants
		SET idempotency_count = idempotency_count + 1
		WHERE tenant_id = ?`, writerTestTenant); err != nil {
		t.Fatalf("stage idempotency ledger mismatch: %v", err)
	}
	stable := readWriterAuthoritySnapshot(t, harness.database)
	_, err := writer.Create(
		harness.actorCtx,
		harness.writeScope,
		capacityCreateRequest("ledger-mismatch-rejected", "ledger-mismatch-reject-0001"),
	)
	if !errors.Is(err, knowledgecatalog.ErrCorrupt) {
		t.Fatalf("Create() with idempotency ledger mismatch error = %v, want ErrCorrupt", err)
	}
	assertWriterAuthoritySnapshotsEqual(t, readWriterAuthoritySnapshot(t, harness.database), stable)
}

func TestWriterIdempotencyReclaimRejectsOversizedOldestKeyBeforeDelete(t *testing.T) {
	harness := newWriterBlackboxHarness(t)
	wallNow := time.Now().UTC().Truncate(time.Microsecond)
	oldWriter, _ := newCapacityTestWriter(
		t,
		harness,
		wallNow.Add(-366*24*time.Hour),
		365*24*time.Hour,
		"ko_reclaim_width_old",
	)
	oldRequest := capacityCreateRequest("reclaim-width-old-anchor", "reclaim-width-old-anchor-01")
	if _, err := oldWriter.Create(harness.actorCtx, harness.writeScope, oldRequest); err != nil {
		t.Fatalf("create reclaim-width old anchor: %v", err)
	}
	const hostileRequestID = "reclaim-width-hostile-00000000"
	seedCapacityReceiptCopies(t, harness.database, capacityReceiptSeed{
		SourceRequestID: oldRequest.GetClientRequestId(),
		RequestIDPrefix: "reclaim-width-hostile-",
		Count:           1,
		Retention:       7 * 24 * time.Hour,
	})

	currentWriter, calls := newCapacityTestWriter(
		t,
		harness,
		wallNow,
		365*24*time.Hour,
		"ko_reclaim_width_current",
	)
	currentRequest := capacityCreateRequest("reclaim-width-current-anchor", "reclaim-width-current-anchor-01")
	if _, err := currentWriter.Create(harness.actorCtx, harness.writeScope, currentRequest); err != nil {
		t.Fatalf("create reclaim-width current anchor: %v", err)
	}
	seedCapacityReceiptCopies(t, harness.database, capacityReceiptSeed{
		SourceRequestID: currentRequest.GetClientRequestId(),
		RequestIDPrefix: "reclaim-width-current-",
		Count:           writerNormalIdempotencyCapacity - 3,
		Retention:       365 * 24 * time.Hour,
	})

	connection, err := harness.database.SQLDB().Conn(t.Context())
	if err != nil {
		t.Fatalf("acquire reclaim-width corruption connection: %v", err)
	}
	if _, err := connection.ExecContext(t.Context(), `
		DROP TRIGGER knowledge_mutation_idempotency_update_is_forbidden`); err != nil {
		_ = connection.Close()
		t.Fatalf("drop immutable receipt update trigger: %v", err)
	}
	if _, err := connection.ExecContext(t.Context(), `PRAGMA foreign_keys = OFF`); err != nil {
		_ = connection.Close()
		t.Fatalf("disable receipt foreign keys: %v", err)
	}
	if _, err := connection.ExecContext(t.Context(), `PRAGMA ignore_check_constraints = ON`); err != nil {
		_ = connection.Close()
		t.Fatalf("disable receipt key checks: %v", err)
	}
	const hostileActorPayloadBytes = 4 << 20
	if _, err := connection.ExecContext(t.Context(), `
		UPDATE knowledge_mutation_idempotency
		SET actor_id = 'WIDE-' || CAST(zeroblob(?) AS TEXT)
		WHERE tenant_id = ? AND actor_kind = 'browser'
		  AND actor_id = 'writer-blackbox-administrator'
		  AND route = 'objects.create' AND client_request_id = ?`,
		hostileActorPayloadBytes,
		writerTestTenant,
		hostileRequestID,
	); err != nil {
		_ = connection.Close()
		t.Fatalf("inject oversized oldest receipt actor: %v", err)
	}
	if _, err := connection.ExecContext(t.Context(), `PRAGMA ignore_check_constraints = OFF`); err != nil {
		_ = connection.Close()
		t.Fatalf("restore receipt key checks: %v", err)
	}
	if _, err := connection.ExecContext(t.Context(), `PRAGMA foreign_keys = ON`); err != nil {
		_ = connection.Close()
		t.Fatalf("restore receipt foreign keys: %v", err)
	}
	if err := connection.Close(); err != nil {
		t.Fatalf("close reclaim-width corruption connection: %v", err)
	}

	hostileBefore := readCapacityReceiptActorBytes(t, harness.database, hostileRequestID)
	if len(hostileBefore) != len("WIDE-")+hostileActorPayloadBytes ||
		!bytes.Equal(hostileBefore[:len("WIDE-")], []byte("WIDE-")) {
		t.Fatalf("hostile oldest actor bytes = %d/prefix %q, want %d/WIDE-", len(hostileBefore), hostileBefore[:min(len(hostileBefore), len("WIDE-"))], len("WIDE-")+hostileActorPayloadBytes)
	}
	stable := readWriterAuthoritySnapshot(t, harness.database)
	if stable.IdempotencyCount != writerNormalIdempotencyCapacity ||
		stable.TableCounts["knowledge_mutation_idempotency"] != writerNormalIdempotencyCapacity {
		t.Fatalf("reclaim-width fixture authority = %#v", stable)
	}

	if _, err := currentWriter.Create(
		harness.actorCtx,
		harness.writeScope,
		capacityCreateRequest("reclaim-width-rejected", "reclaim-width-rejected-0001"),
	); !errors.Is(err, knowledgecatalog.ErrCorrupt) {
		t.Fatalf("Create() with oversized oldest receipt key error = %v, want ErrCorrupt", err)
	}
	if calls.IDs.Load() != 1 || calls.Clocks.Load() != 1 {
		t.Fatalf("generator calls after oversized reclaim rejection = IDs %d clocks %d, want 1/1", calls.IDs.Load(), calls.Clocks.Load())
	}
	assertWriterAuthoritySnapshotsEqual(t, readWriterAuthoritySnapshot(t, harness.database), stable)
	hostileAfter := readCapacityReceiptActorBytes(t, harness.database, hostileRequestID)
	if !bytes.Equal(hostileAfter, hostileBefore) {
		t.Fatalf("oversized oldest receipt changed across failed reclaim: before %d bytes, after %d", len(hostileBefore), len(hostileAfter))
	}
	assertCapacityReceiptExists(t, harness.database, oldRequest.GetClientRequestId())
	assertCapacityReceiptExists(t, harness.database, currentRequest.GetClientRequestId())
}

func TestWriterIdempotencyReclaimSemanticPreflightRejectsEqualWidthCorruptKeys(t *testing.T) {
	harness := newWriterBlackboxHarness(t)
	wallNow := time.Now().UTC().Truncate(time.Microsecond)
	oldWriter, _ := newCapacityTestWriter(
		t,
		harness,
		wallNow.Add(-366*24*time.Hour),
		365*24*time.Hour,
		"ko_reclaim_semantic_old",
	)
	oldRequest := capacityCreateRequest("reclaim-semantic-old-anchor", "reclaim-semantic-old-anchor-01")
	if _, err := oldWriter.Create(harness.actorCtx, harness.writeScope, oldRequest); err != nil {
		t.Fatalf("create reclaim-semantic old anchor: %v", err)
	}
	const hostileRequestID = "reclaim-semantic-hostile-00000000"
	seedCapacityReceiptCopies(t, harness.database, capacityReceiptSeed{
		SourceRequestID: oldRequest.GetClientRequestId(),
		RequestIDPrefix: "reclaim-semantic-hostile-",
		Count:           1,
		Retention:       7 * 24 * time.Hour,
	})

	currentWriter, calls := newCapacityTestWriter(
		t,
		harness,
		wallNow,
		365*24*time.Hour,
		"ko_reclaim_semantic_current",
	)
	currentRequest := capacityCreateRequest("reclaim-semantic-current-anchor", "reclaim-semantic-current-anchor-01")
	if _, err := currentWriter.Create(harness.actorCtx, harness.writeScope, currentRequest); err != nil {
		t.Fatalf("create reclaim-semantic current anchor: %v", err)
	}
	seedCapacityReceiptCopies(t, harness.database, capacityReceiptSeed{
		SourceRequestID: currentRequest.GetClientRequestId(),
		RequestIDPrefix: "reclaim-semantic-current-",
		Count:           writerNormalIdempotencyCapacity - 3,
		Retention:       365 * 24 * time.Hour,
	})

	connection, err := harness.database.SQLDB().Conn(t.Context())
	if err != nil {
		t.Fatalf("acquire reclaim-semantic corruption connection: %v", err)
	}
	if _, err := connection.ExecContext(t.Context(), `
		DROP TRIGGER knowledge_mutation_idempotency_update_is_forbidden`); err != nil {
		_ = connection.Close()
		t.Fatalf("drop immutable receipt update trigger: %v", err)
	}
	if _, err := connection.ExecContext(t.Context(), `
		DROP TRIGGER audit_event_update_is_forbidden`); err != nil {
		_ = connection.Close()
		t.Fatalf("drop audit event update trigger: %v", err)
	}

	var auditSequence int64
	if err := connection.QueryRowContext(t.Context(), `
		SELECT successful_audit_sequence
		FROM knowledge_mutation_idempotency
		WHERE tenant_id = ? AND route = 'objects.create'
		  AND client_request_id = ?`, writerTestTenant, hostileRequestID).Scan(&auditSequence); err != nil {
		_ = connection.Close()
		t.Fatalf("read semantic reclaim audit sequence: %v", err)
	}
	alteredActorID := strings.Repeat("a", len("writer-blackbox-administrator"))
	alteredRequestID := strings.Repeat("!", len(hostileRequestID))
	alteredDigest := bytes.Repeat([]byte{0xa5}, sha256.Size)
	tests := []struct {
		name             string
		requestID        string
		corruptRequestID string
		corruptSQL       string
		corruptArgs      []any
		restoreSQL       string
		restoreArgs      []any
		corruptAuditSQL  string
		corruptAuditArgs []any
		restoreAuditSQL  string
		restoreAuditArgs []any
	}{
		{
			name:             "actor identity",
			requestID:        "reclaim-semantic-actor-0001",
			corruptRequestID: hostileRequestID,
			corruptSQL: `UPDATE knowledge_mutation_idempotency
				SET actor_id = ?
				WHERE tenant_id = ? AND client_request_id = ?`,
			corruptArgs: []any{alteredActorID, writerTestTenant, hostileRequestID},
			restoreSQL: `UPDATE knowledge_mutation_idempotency
				SET actor_id = 'writer-blackbox-administrator'
				WHERE tenant_id = ? AND client_request_id = ?`,
			restoreArgs: []any{writerTestTenant, hostileRequestID},
			corruptAuditSQL: `UPDATE audit_events SET actor_id = ?
				WHERE tenant_id = ? AND sequence = ?`,
			corruptAuditArgs: []any{alteredActorID, writerTestTenant, auditSequence},
			restoreAuditSQL: `UPDATE audit_events SET actor_id = 'writer-blackbox-administrator'
				WHERE tenant_id = ? AND sequence = ?`,
			restoreAuditArgs: []any{writerTestTenant, auditSequence},
		},
		{
			name:             "client request identity",
			requestID:        "reclaim-semantic-request-0001",
			corruptRequestID: alteredRequestID,
			corruptSQL: `UPDATE knowledge_mutation_idempotency
				SET client_request_id = ?
				WHERE tenant_id = ? AND client_request_id = ?`,
			corruptArgs: []any{alteredRequestID, writerTestTenant, hostileRequestID},
			restoreSQL: `UPDATE knowledge_mutation_idempotency
				SET client_request_id = ?
				WHERE tenant_id = ? AND client_request_id = ?`,
			restoreArgs: []any{hostileRequestID, writerTestTenant, alteredRequestID},
		},
		{
			name:             "request digest",
			requestID:        "reclaim-semantic-digest-0001",
			corruptRequestID: hostileRequestID,
			corruptSQL: `UPDATE knowledge_mutation_idempotency
				SET request_digest = ?
				WHERE tenant_id = ? AND client_request_id = ?`,
			corruptArgs: []any{alteredDigest, writerTestTenant, hostileRequestID},
			restoreSQL: `UPDATE knowledge_mutation_idempotency
				SET request_digest = (
					SELECT committed.request_digest
					FROM knowledge_mutation_commit_authorities AS committed
					WHERE committed.tenant_id = knowledge_mutation_idempotency.tenant_id
					  AND committed.catalog_revision = knowledge_mutation_idempotency.committed_catalog_revision
				)
				WHERE tenant_id = ? AND client_request_id = ?`,
			restoreArgs: []any{writerTestTenant, hostileRequestID},
		},
	}
	if err := connection.Close(); err != nil {
		t.Fatalf("close reclaim-semantic setup connection: %v", err)
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			caseConnection, err := harness.database.SQLDB().Conn(t.Context())
			if err != nil {
				t.Fatalf("open %s corruption connection: %v", test.name, err)
			}
			defer caseConnection.Close()
			assertCapacityForeignKeys(t, caseConnection, 1)
			if _, err := caseConnection.ExecContext(t.Context(), test.corruptSQL, test.corruptArgs...); err == nil {
				t.Fatalf("row-only %s corruption succeeded with foreign keys enabled", test.name)
			}
			if _, err := caseConnection.ExecContext(t.Context(), `PRAGMA foreign_keys = OFF`); err != nil {
				t.Fatalf("disable %s corruption foreign keys: %v", test.name, err)
			}
			assertCapacityForeignKeys(t, caseConnection, 0)
			corrupted := false
			defer func() {
				if !corrupted {
					return
				}
				_, _ = caseConnection.ExecContext(t.Context(), `PRAGMA foreign_keys = OFF`)
				_, _ = caseConnection.ExecContext(t.Context(), test.restoreSQL, test.restoreArgs...)
				if test.restoreAuditSQL != "" {
					_, _ = caseConnection.ExecContext(t.Context(), test.restoreAuditSQL, test.restoreAuditArgs...)
				}
				_, _ = caseConnection.ExecContext(t.Context(), `PRAGMA foreign_keys = ON`)
			}()
			if test.corruptAuditSQL != "" {
				assertOneCapacityRowUpdated(t, caseConnection, test.corruptAuditSQL, test.corruptAuditArgs...)
			}
			assertOneCapacityRowUpdated(t, caseConnection, test.corruptSQL, test.corruptArgs...)
			corrupted = true
			if _, err := caseConnection.ExecContext(t.Context(), `PRAGMA foreign_keys = ON`); err != nil {
				t.Fatalf("restore %s corruption foreign keys: %v", test.name, err)
			}
			assertCapacityForeignKeys(t, caseConnection, 1)
			corruptKey := readOldestCapacityReceiptKey(t, harness.database)
			if corruptKey.ClientRequestID != test.corruptRequestID {
				t.Fatalf("oldest %s receipt = %#v, want request %q", test.name, corruptKey, test.corruptRequestID)
			}
			stable := readWriterAuthoritySnapshot(t, harness.database)
			if stable.IdempotencyCount != writerNormalIdempotencyCapacity {
				t.Fatalf("semantic reclaim fixture idempotency count = %d, want %d", stable.IdempotencyCount, writerNormalIdempotencyCapacity)
			}

			if _, err := currentWriter.Create(
				harness.actorCtx,
				harness.writeScope,
				capacityCreateRequest("semantic-reclaim-rejected-"+test.name, test.requestID),
			); !errors.Is(err, knowledgecatalog.ErrCorrupt) {
				t.Fatalf("Create() with equal-width corrupt %s error = %v, want ErrCorrupt", test.name, err)
			}
			assertWriterAuthoritySnapshotsEqual(t, readWriterAuthoritySnapshot(t, harness.database), stable)
			if after := readOldestCapacityReceiptKey(t, harness.database); after != corruptKey {
				t.Fatalf("oldest corrupt key changed across failed reclaim:\n before %#v\n  after %#v", corruptKey, after)
			}
			assertOneCapacityRowUpdated(t, caseConnection, test.restoreSQL, test.restoreArgs...)
			if test.restoreAuditSQL != "" {
				assertOneCapacityRowUpdated(t, caseConnection, test.restoreAuditSQL, test.restoreAuditArgs...)
			}
			corrupted = false
		})
	}
	if calls.IDs.Load() != 1 || calls.Clocks.Load() != 1 {
		t.Fatalf("generator calls after semantic reclaim rejection = IDs %d clocks %d, want 1/1", calls.IDs.Load(), calls.Clocks.Load())
	}
	assertCapacityReceiptExists(t, harness.database, hostileRequestID)
	assertWriterCatalogIntegrity(t, harness.database)
}

func TestWriterIdempotencyReclaimCannotSkipCorruptExpiredPrefixForLaterValidReceipt(t *testing.T) {
	harness := newWriterBlackboxHarness(t)
	wallNow := time.Now().UTC().Truncate(time.Microsecond)
	oldWriter, _ := newCapacityTestWriter(
		t,
		harness,
		wallNow.Add(-366*24*time.Hour),
		365*24*time.Hour,
		"ko_reclaim_prefix_old",
	)
	oldRequest := capacityCreateRequest("reclaim-prefix-old-anchor", "reclaim-prefix-old-anchor-0001")
	if _, err := oldWriter.Create(harness.actorCtx, harness.writeScope, oldRequest); err != nil {
		t.Fatalf("create reclaim-prefix old anchor: %v", err)
	}
	const corruptRequestID = "000-reclaim-prefix-corrupt-00000000"
	seedCapacityReceiptCopies(t, harness.database, capacityReceiptSeed{
		SourceRequestID: oldRequest.GetClientRequestId(),
		RequestIDPrefix: "000-reclaim-prefix-corrupt-",
		Count:           1,
		Retention:       7 * 24 * time.Hour,
	})

	currentWriter, calls := newCapacityTestWriter(
		t,
		harness,
		wallNow,
		365*24*time.Hour,
		"ko_reclaim_prefix_current",
	)
	currentRequest := capacityCreateRequest("reclaim-prefix-current-anchor", "reclaim-prefix-current-anchor-0001")
	if _, err := currentWriter.Create(harness.actorCtx, harness.writeScope, currentRequest); err != nil {
		t.Fatalf("create reclaim-prefix current anchor: %v", err)
	}
	seedCapacityReceiptCopies(t, harness.database, capacityReceiptSeed{
		SourceRequestID: currentRequest.GetClientRequestId(),
		RequestIDPrefix: "reclaim-prefix-current-filler-",
		Count:           writerNormalIdempotencyCapacity - 3,
		Retention:       365 * 24 * time.Hour,
	})

	connection, err := harness.database.SQLDB().Conn(t.Context())
	if err != nil {
		t.Fatalf("acquire reclaim-prefix corruption connection: %v", err)
	}
	defer connection.Close()
	var receiptTriggerSQL string
	if err := connection.QueryRowContext(t.Context(), `
		SELECT sql FROM sqlite_schema
		WHERE type = 'trigger'
		  AND name = 'knowledge_mutation_idempotency_update_is_forbidden'`).Scan(&receiptTriggerSQL); err != nil {
		t.Fatalf("read receipt immutability trigger: %v", err)
	}
	var originalToken []byte
	if err := connection.QueryRowContext(t.Context(), `
		SELECT committed_catalog_state_token
		FROM knowledge_mutation_idempotency
		WHERE tenant_id = ? AND route = 'objects.create'
		  AND client_request_id = ?`,
		writerTestTenant,
		corruptRequestID,
	).Scan(&originalToken); err != nil {
		t.Fatalf("read reclaim-prefix token: %v", err)
	}
	if len(originalToken) != 32 {
		t.Fatalf("reclaim-prefix state token bytes = %d, want 32", len(originalToken))
	}
	if _, err := connection.ExecContext(t.Context(), `PRAGMA foreign_keys = OFF`); err != nil {
		t.Fatalf("disable reclaim-prefix foreign keys: %v", err)
	}
	if _, err := connection.ExecContext(t.Context(), `
		DROP TRIGGER knowledge_mutation_idempotency_update_is_forbidden`); err != nil {
		t.Fatalf("drop receipt immutability trigger: %v", err)
	}
	corruptToken := bytes.Repeat([]byte{0x5a}, len(originalToken))
	if bytes.Equal(corruptToken, originalToken) {
		corruptToken[0] ^= 0xff
	}
	assertOneCapacityRowUpdated(t, connection, `
		UPDATE knowledge_mutation_idempotency
		SET committed_catalog_state_token = ?
		WHERE tenant_id = ? AND route = 'objects.create'
		  AND client_request_id = ?`,
		corruptToken,
		writerTestTenant,
		corruptRequestID,
	)
	if _, err := connection.ExecContext(t.Context(), receiptTriggerSQL); err != nil {
		t.Fatalf("restore receipt immutability trigger: %v", err)
	}
	if _, err := connection.ExecContext(t.Context(), `PRAGMA foreign_keys = ON`); err != nil {
		t.Fatalf("restore reclaim-prefix foreign keys: %v", err)
	}

	oldest := readOldestCapacityReceiptKey(t, harness.database)
	if oldest.ClientRequestID != corruptRequestID {
		t.Fatalf("oldest reclaim-prefix receipt = %#v, want corrupt prefix", oldest)
	}
	stable := readWriterAuthoritySnapshot(t, harness.database)
	if stable.IdempotencyCount != writerNormalIdempotencyCapacity {
		t.Fatalf("reclaim-prefix fixture idempotency count = %d, want %d", stable.IdempotencyCount, writerNormalIdempotencyCapacity)
	}
	if response, err := currentWriter.Create(
		harness.actorCtx,
		harness.writeScope,
		capacityCreateRequest("reclaim-prefix-rejected", "reclaim-prefix-rejected-0001"),
	); response != nil || !errors.Is(err, knowledgecatalog.ErrCorrupt) {
		t.Fatalf("Create() with skipped corrupt reclaim prefix = (%v, %v), want nil/ErrCorrupt", response, err)
	}
	if calls.IDs.Load() != 1 || calls.Clocks.Load() != 1 {
		t.Fatalf("reclaim-prefix rejection reached generators: IDs=%d clocks=%d", calls.IDs.Load(), calls.Clocks.Load())
	}
	assertWriterAuthoritySnapshotsEqual(t, readWriterAuthoritySnapshot(t, harness.database), stable)
	assertCapacityReceiptExists(t, harness.database, corruptRequestID)
	assertCapacityReceiptExists(t, harness.database, oldRequest.GetClientRequestId())

	if _, err := connection.ExecContext(t.Context(), `PRAGMA foreign_keys = OFF`); err != nil {
		t.Fatalf("disable foreign keys for reclaim-prefix restore: %v", err)
	}
	if _, err := connection.ExecContext(t.Context(), `
		DROP TRIGGER knowledge_mutation_idempotency_update_is_forbidden`); err != nil {
		t.Fatalf("drop receipt trigger for reclaim-prefix restore: %v", err)
	}
	assertOneCapacityRowUpdated(t, connection, `
		UPDATE knowledge_mutation_idempotency
		SET committed_catalog_state_token = ?
		WHERE tenant_id = ? AND route = 'objects.create'
		  AND client_request_id = ?`,
		originalToken,
		writerTestTenant,
		corruptRequestID,
	)
	if _, err := connection.ExecContext(t.Context(), receiptTriggerSQL); err != nil {
		t.Fatalf("restore final receipt immutability trigger: %v", err)
	}
	if _, err := connection.ExecContext(t.Context(), `PRAGMA foreign_keys = ON`); err != nil {
		t.Fatalf("restore final reclaim-prefix foreign keys: %v", err)
	}
	assertWriterCatalogIntegrity(t, harness.database)
}

func TestWriterReclaimsSchemaValidExpiredQuarantineReceiptAtNormalCapacity(t *testing.T) {
	harness := newWriterBlackboxHarness(t)
	wallNow := time.Now().UTC().Truncate(time.Microsecond)
	oldTime := wallNow.Add(-366 * 24 * time.Hour)
	oldWriter, _ := newCapacityTestWriter(
		t,
		harness,
		oldTime,
		365*24*time.Hour,
		"ko_quarantine_reclaim_old",
	)
	oldRequest := capacityCreateRequest("quarantine-reclaim-old", "quarantine-reclaim-old-0001")
	oldResponse, err := oldWriter.Create(harness.actorCtx, harness.writeScope, oldRequest)
	if err != nil {
		t.Fatalf("create quarantine reclaim old object: %v", err)
	}
	const quarantineRequestID = "quarantine-reclaim-request-0001"
	stageSchemaValidExpiredQuarantineReceipt(
		t,
		harness.database,
		oldResponse.GetKnowledgeObject().GetKnowledgeObjectId(),
		oldTime.Add(time.Microsecond),
		quarantineRequestID,
	)

	currentWriter, _ := newCapacityTestWriter(
		t,
		harness,
		wallNow,
		365*24*time.Hour,
		"ko_quarantine_reclaim_current",
	)
	currentRequest := capacityCreateRequest("quarantine-reclaim-current", "quarantine-reclaim-current-0001")
	if _, err := currentWriter.Create(harness.actorCtx, harness.writeScope, currentRequest); err != nil {
		t.Fatalf("create quarantine reclaim current anchor: %v", err)
	}
	seedCapacityReceiptCopies(t, harness.database, capacityReceiptSeed{
		SourceRequestID: currentRequest.GetClientRequestId(),
		RequestIDPrefix: "quarantine-reclaim-filler-",
		Count:           writerNormalIdempotencyCapacity - 3,
		Retention:       365 * 24 * time.Hour,
	})
	before := readWriterAuthoritySnapshot(t, harness.database)
	if before.IdempotencyCount != writerNormalIdempotencyCapacity ||
		before.TableCounts["knowledge_mutation_idempotency"] != writerNormalIdempotencyCapacity {
		t.Fatalf("quarantine reclaim fixture authority = %#v", before)
	}
	assertCapacityReceiptRouteCount(t, harness.database, "objects.quarantine", quarantineRequestID, 1)
	oldest := readOldestCapacityReceiptKey(t, harness.database)
	if oldest.ActorKind != "system" || oldest.ActorID != "open-splunk-server" ||
		oldest.Route != "objects.quarantine" || oldest.ClientRequestID != quarantineRequestID ||
		oldest.MutationKind != "quarantine" {
		t.Fatalf("oldest schema-valid quarantine receipt = %#v", oldest)
	}

	newRequest := capacityCreateRequest("quarantine-reclaim-success", "quarantine-reclaim-success-0001")
	if _, err := currentWriter.Create(harness.actorCtx, harness.writeScope, newRequest); err != nil {
		t.Fatalf("Create() after expired quarantine receipt: %v", err)
	}
	after := readWriterAuthoritySnapshot(t, harness.database)
	if after.IdempotencyCount != writerNormalIdempotencyCapacity ||
		after.CatalogRevision != before.CatalogRevision+1 ||
		after.VersionCount != before.VersionCount+1 ||
		after.IdentityCount != before.IdentityCount+1 ||
		after.AuditNextSequence != before.AuditNextSequence+1 ||
		after.AuditEventCount != before.AuditEventCount+1 {
		t.Fatalf("authority after quarantine receipt reclaim = %#v, before %#v", after, before)
	}
	assertCapacityReceiptRouteCount(t, harness.database, "objects.quarantine", quarantineRequestID, 0)
	assertCapacityReceiptExists(t, harness.database, oldRequest.GetClientRequestId())
	assertCapacityReceiptExists(t, harness.database, currentRequest.GetClientRequestId())
	assertCapacityReceiptExists(t, harness.database, newRequest.GetClientRequestId())
	assertReceiptPrefixCount(
		t,
		harness.database,
		"quarantine-reclaim-filler-",
		writerNormalIdempotencyCapacity-3,
	)
	var recoveryCount int
	if err := harness.database.SQLDB().QueryRowContext(t.Context(), `
		SELECT count(*) FROM knowledge_recovery_audit
		WHERE tenant_id = ? AND knowledge_object_id = ?
		  AND object_version = 2 AND recovery_reason = 'root_corruption'`,
		writerTestTenant,
		oldResponse.GetKnowledgeObject().GetKnowledgeObjectId(),
	).Scan(&recoveryCount); err != nil {
		t.Fatalf("count retained quarantine recovery audit: %v", err)
	}
	if recoveryCount != 1 {
		t.Fatalf("retained quarantine recovery audit count = %d, want 1", recoveryCount)
	}
	assertWriterCatalogIntegrity(t, harness.database)
}

type capacityReceiptSeed struct {
	SourceRequestID string
	RequestIDPrefix string
	Count           int
	Retention       time.Duration
}

func stageSchemaValidExpiredQuarantineReceipt(
	t *testing.T,
	database *control.DB,
	objectID string,
	quarantinedAt time.Time,
	requestID string,
) {
	t.Helper()
	tx, err := database.SQLDB().BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatalf("begin schema-valid quarantine publication: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	var appID, ownerID, objectType, name, sharingScope, state string
	var currentVersion, createdAtUnixMicro int64
	if err := tx.QueryRowContext(t.Context(), `
		SELECT app_id, owner_id, object_type, name, sharing_scope,
		       state, current_version, created_at_unix_micro
		FROM knowledge_objects
		WHERE tenant_id = ? AND knowledge_object_id = ?`,
		writerTestTenant,
		objectID,
	).Scan(
		&appID,
		&ownerID,
		&objectType,
		&name,
		&sharingScope,
		&state,
		&currentVersion,
		&createdAtUnixMicro,
	); err != nil {
		t.Fatalf("read quarantine source registry: %v", err)
	}
	quarantinedAtUnixMicro := quarantinedAt.UTC().UnixMicro()
	if currentVersion != 1 || state != "draft" || quarantinedAtUnixMicro <= createdAtUnixMicro {
		t.Fatalf("invalid quarantine source authority: version=%d state=%q created=%d quarantine=%d", currentVersion, state, createdAtUnixMicro, quarantinedAtUnixMicro)
	}

	assertOneCapacityRowUpdated(t, tx, `
		INSERT INTO knowledge_object_versions (
			tenant_id, knowledge_object_id, object_version,
			app_id, owner_id, object_type, name, sharing_scope, state,
			definition_digest, dependency_count, mutation_kind,
			quarantine_reason, created_at_unix_micro
		) VALUES (?, ?, 2, ?, ?, ?, ?, ?, 'quarantined',
		          NULL, 0, 'quarantine', 'root_corruption', ?)`,
		writerTestTenant,
		objectID,
		appID,
		ownerID,
		objectType,
		name,
		sharingScope,
		quarantinedAtUnixMicro,
	)
	assertOneCapacityRowUpdated(t, tx, `
		INSERT INTO knowledge_object_dependency_seals (
			tenant_id, knowledge_object_id, object_version, dependency_count
		) VALUES (?, ?, 2, 0)`, writerTestTenant, objectID)
	assertOneCapacityRowUpdated(t, tx, `
		INSERT INTO knowledge_object_list_projections (
			tenant_id, knowledge_object_id, object_version,
			app_id, owner_id, object_type, name, sharing_scope, state,
			description_present, description,
			index_selector_count, host_selector_count,
			source_selector_count, sourcetype_selector_count,
			selector_value_bytes, canonical_selector_bytes
		) VALUES (?, ?, 2, ?, ?, ?, ?, ?, 'quarantined',
		          0, '', 0, 0, 0, 0, 0, 0)`,
		writerTestTenant,
		objectID,
		appID,
		ownerID,
		objectType,
		name,
		sharingScope,
	)
	assertOneCapacityRowUpdated(t, tx, `
		INSERT INTO knowledge_object_list_projection_seals (
			tenant_id, knowledge_object_id, object_version,
			projection_bytes, canonical_selector_bytes
		) VALUES (?, ?, 2, 0, 0)`, writerTestTenant, objectID)
	assertOneCapacityRowUpdated(t, tx, `
		UPDATE knowledge_objects
		SET current_version = 2,
		    state = 'quarantined',
		    definition_digest = NULL,
		    updated_at_unix_micro = ?,
		    disabled_at_unix_micro = NULL,
		    quarantined_at_unix_micro = ?,
		    deleted_at_unix_micro = NULL,
		    quarantine_reason = 'root_corruption'
		WHERE tenant_id = ? AND knowledge_object_id = ?
		  AND current_version = 1 AND state = 'draft'`,
		quarantinedAtUnixMicro,
		quarantinedAtUnixMicro,
		writerTestTenant,
		objectID,
	)
	assertOneCapacityRowUpdated(t, tx, `
		UPDATE knowledge_catalog_tenants
		SET catalog_revision = catalog_revision + 1
		WHERE tenant_id = ?`, writerTestTenant)
	assertOneCapacityRowUpdated(t, tx, `
		INSERT INTO knowledge_recovery_audit (
			tenant_id, sequence, knowledge_object_id, object_version,
			actor_kind, actor_id, actor_role, app_id, object_type,
			sharing_scope, recovery_reason, occurred_at_unix_micro
		) VALUES (?, 1, ?, 2, 'system', 'open-splunk-server', 'system',
		          ?, ?, ?, 'root_corruption', ?)`,
		writerTestTenant,
		objectID,
		appID,
		objectType,
		sharingScope,
		quarantinedAtUnixMicro,
	)

	var revision int64
	var token []byte
	if err := tx.QueryRowContext(t.Context(), `
		SELECT tenant.catalog_revision, head.state_token
		FROM knowledge_catalog_tenants AS tenant
		JOIN knowledge_catalog_revision_heads AS head
		  ON head.tenant_id = tenant.tenant_id
		 AND head.catalog_revision = tenant.catalog_revision
		WHERE tenant.tenant_id = ?`, writerTestTenant).Scan(&revision, &token); err != nil {
		t.Fatalf("read quarantine commit state: %v", err)
	}
	if revision < 1 || len(token) != 32 {
		t.Fatalf("quarantine commit state = revision %d token %d bytes", revision, len(token))
	}
	quarantineRetainUntil := quarantinedAtUnixMicro + int64(7*24*time.Hour/time.Microsecond)
	assertOneCapacityRowUpdated(t, tx, `
		INSERT INTO knowledge_mutation_commit_authorities (
			tenant_id, actor_kind, actor_id, route, client_request_id,
			request_digest, catalog_revision, catalog_state_token, mutation_kind,
			knowledge_object_id, object_version, occurred_at_unix_micro,
			retention_anchor_unix_micro, retain_until_unix_micro,
			successful_audit_sequence, recovery_audit_sequence
		) VALUES (?, 'system', 'open-splunk-server', 'objects.quarantine', ?,
		          zeroblob(32), ?, ?, 'quarantine', ?, 2, ?, ?, ?, NULL, 1)`,
		writerTestTenant,
		requestID,
		revision,
		token,
		objectID,
		quarantinedAtUnixMicro,
		quarantinedAtUnixMicro,
		quarantineRetainUntil,
	)
	quarantineOutcome, err := (proto.MarshalOptions{Deterministic: true}).Marshal(
		&opensplunkv1.KnowledgeMutationOutcomeRecord{
			Route:        "objects.quarantine",
			MutationKind: "quarantine",
			Object: &opensplunkv1.KnowledgeObjectVersionReference{
				KnowledgeObjectId: objectID,
				Version:           2,
			},
			TenantCatalogRevision:   uint64(revision),
			TenantCatalogStateToken: bytes.Clone(token),
			AuditAuthority: &opensplunkv1.KnowledgeMutationOutcomeRecord_RecoveryAuditSequence{
				RecoveryAuditSequence: 1,
			},
			OccurredAtUnixMicro:      quarantinedAtUnixMicro,
			RetentionAnchorUnixMicro: quarantinedAtUnixMicro,
			RetainUntilUnixMicro:     quarantineRetainUntil,
		},
	)
	if err != nil {
		t.Fatalf("encode quarantine outcome authority: %v", err)
	}
	assertOneCapacityRowUpdated(t, tx, `
		INSERT INTO knowledge_mutation_idempotency (
			tenant_id, actor_kind, actor_id, route, client_request_id,
			mutation_kind, request_digest_format_version, request_digest,
			outcome_format_version, outcome_proto,
			committed_catalog_revision, committed_catalog_state_token,
			knowledge_object_id, object_version,
			successful_audit_sequence, recovery_audit_sequence,
			created_at_unix_micro, retention_anchor_unix_micro,
			retain_until_unix_micro
		) VALUES (?, 'system', 'open-splunk-server', 'objects.quarantine', ?,
		          'quarantine', 1, zeroblob(32), 1, ?, ?, ?, ?, 2,
		          NULL, 1, ?, ?, ?)`,
		writerTestTenant,
		requestID,
		quarantineOutcome,
		revision,
		token,
		objectID,
		quarantinedAtUnixMicro,
		quarantinedAtUnixMicro,
		quarantineRetainUntil,
	)
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit schema-valid quarantine publication: %v", err)
	}
}

func seedCapacityReceiptCopies(t *testing.T, database *control.DB, seed capacityReceiptSeed) {
	t.Helper()
	if seed.Count <= 0 {
		return
	}
	if seed.SourceRequestID == "" || seed.RequestIDPrefix == "" ||
		seed.Retention < 7*24*time.Hour || seed.Retention > 365*24*time.Hour {
		t.Fatalf("invalid capacity receipt seed: %#v", seed)
	}
	tx, err := database.SQLDB().BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatalf("begin capacity receipt seed: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	var receiptUpdateTriggerSQL string
	if err := tx.QueryRowContext(t.Context(), `
		SELECT sql FROM sqlite_schema
		WHERE type = 'trigger'
		  AND name = 'knowledge_mutation_idempotency_update_is_forbidden'`).Scan(&receiptUpdateTriggerSQL); err != nil {
		t.Fatalf("read receipt immutability trigger: %v", err)
	}
	var commitUpdateTriggerSQL string
	if err := tx.QueryRowContext(t.Context(), `
		SELECT sql FROM sqlite_schema
		WHERE type = 'trigger'
		  AND name = 'knowledge_mutation_commit_authority_update_is_forbidden'`).Scan(&commitUpdateTriggerSQL); err != nil {
		t.Fatalf("read commit-authority immutability trigger: %v", err)
	}
	var commitExactTriggerSQL string
	if err := tx.QueryRowContext(t.Context(), `
		SELECT sql FROM sqlite_schema
		WHERE type = 'trigger'
		  AND name = 'knowledge_mutation_commit_authority_is_exact'`).Scan(&commitExactTriggerSQL); err != nil {
		t.Fatalf("read commit-authority exactness trigger: %v", err)
	}
	var commitCollisionTriggerSQL string
	if err := tx.QueryRowContext(t.Context(), `
		SELECT sql FROM sqlite_schema
		WHERE type = 'trigger'
		  AND name = 'knowledge_mutation_commit_authority_collision_is_forbidden'`).Scan(&commitCollisionTriggerSQL); err != nil {
		t.Fatalf("read commit-authority collision trigger: %v", err)
	}
	receiptInsertTriggerNames := []string{
		"knowledge_mutation_idempotency_capacity_is_available",
		"knowledge_mutation_idempotency_identity_collision_is_forbidden",
		"knowledge_mutation_idempotency_matches_commit_authority",
		"knowledge_mutation_idempotency_matches_audit_authority",
		"knowledge_mutation_idempotency_after_insert",
	}
	receiptInsertTriggerSQL := make([]string, 0, len(receiptInsertTriggerNames))
	for _, name := range receiptInsertTriggerNames {
		var statement string
		if err := tx.QueryRowContext(t.Context(), `
			SELECT sql FROM sqlite_schema WHERE type = 'trigger' AND name = ?`, name).Scan(&statement); err != nil {
			t.Fatalf("read %s trigger: %v", name, err)
		}
		receiptInsertTriggerSQL = append(receiptInsertTriggerSQL, statement)
	}
	var sourceOutcome []byte
	var sourceOccurred int64
	var sourceRevision int64
	if err := tx.QueryRowContext(t.Context(), `
		SELECT outcome_proto, created_at_unix_micro, committed_catalog_revision
		FROM knowledge_mutation_idempotency
		WHERE tenant_id = ? AND actor_kind = 'browser'
		  AND actor_id = 'writer-blackbox-administrator'
		  AND route = 'objects.create' AND client_request_id = ?`,
		writerTestTenant,
		seed.SourceRequestID,
	).Scan(&sourceOutcome, &sourceOccurred, &sourceRevision); err != nil {
		t.Fatalf("read source capacity receipt: %v", err)
	}
	envelope := &opensplunkv1.KnowledgeMutationOutcomeRecord{}
	if err := proto.Unmarshal(sourceOutcome, envelope); err != nil {
		t.Fatalf("decode source capacity outcome: %v", err)
	}
	retainUntil := sourceOccurred + seed.Retention.Microseconds()
	envelope.OccurredAtUnixMicro = sourceOccurred
	envelope.RetentionAnchorUnixMicro = sourceOccurred
	envelope.RetainUntilUnixMicro = retainUntil
	canonicalOutcome, err := (proto.MarshalOptions{Deterministic: true}).Marshal(envelope)
	if err != nil {
		t.Fatalf("encode source capacity outcome: %v", err)
	}
	if _, err := tx.ExecContext(t.Context(), `
		DROP TRIGGER knowledge_mutation_idempotency_update_is_forbidden`); err != nil {
		t.Fatalf("drop receipt immutability trigger for historical fixture: %v", err)
	}
	if _, err := tx.ExecContext(t.Context(), `
		DROP TRIGGER knowledge_mutation_commit_authority_update_is_forbidden`); err != nil {
		t.Fatalf("drop commit-authority immutability trigger for historical fixture: %v", err)
	}
	if _, err := tx.ExecContext(t.Context(), `
		DROP TRIGGER knowledge_mutation_commit_authority_is_exact`); err != nil {
		t.Fatalf("drop commit-authority exactness trigger for synthetic fixture: %v", err)
	}
	if _, err := tx.ExecContext(t.Context(), `
		DROP TRIGGER knowledge_mutation_commit_authority_collision_is_forbidden`); err != nil {
		t.Fatalf("drop commit-authority collision trigger for synthetic fixture: %v", err)
	}
	if _, err := tx.ExecContext(t.Context(), `
		UPDATE knowledge_mutation_commit_authorities
		SET retention_anchor_unix_micro = ?, retain_until_unix_micro = ?
		WHERE tenant_id = ? AND catalog_revision = ?`,
		sourceOccurred,
		retainUntil,
		writerTestTenant,
		sourceRevision,
	); err != nil {
		t.Fatalf("stage historical source commit authority: %v", err)
	}
	if _, err := tx.ExecContext(t.Context(), `
		UPDATE knowledge_mutation_idempotency
		SET outcome_proto = ?, retention_anchor_unix_micro = ?,
		    retain_until_unix_micro = ?
		WHERE tenant_id = ? AND actor_kind = 'browser'
		  AND actor_id = 'writer-blackbox-administrator'
		  AND route = 'objects.create' AND client_request_id = ?`,
		canonicalOutcome,
		sourceOccurred,
		retainUntil,
		writerTestTenant,
		seed.SourceRequestID,
	); err != nil {
		t.Fatalf("stage historical source capacity receipt: %v", err)
	}
	for _, name := range receiptInsertTriggerNames {
		if _, err := tx.ExecContext(t.Context(), "DROP TRIGGER "+name); err != nil {
			t.Fatalf("drop %s for bounded synthetic fixture: %v", name, err)
		}
	}
	const syntheticRevisionFloor int64 = 4_000_000_000_000_000_000
	var maximumSyntheticRevision sql.NullInt64
	if err := tx.QueryRowContext(t.Context(), `
		SELECT max(catalog_revision)
		FROM knowledge_mutation_commit_authorities
		WHERE tenant_id = ? AND catalog_revision >= ?`,
		writerTestTenant,
		syntheticRevisionFloor,
	).Scan(&maximumSyntheticRevision); err != nil {
		t.Fatalf("read synthetic capacity revision floor: %v", err)
	}
	firstSyntheticRevision := syntheticRevisionFloor
	if maximumSyntheticRevision.Valid {
		firstSyntheticRevision = maximumSyntheticRevision.Int64 + 1
	}
	if firstSyntheticRevision < syntheticRevisionFloor ||
		firstSyntheticRevision > syntheticRevisionFloor+1_000_000-int64(seed.Count) {
		t.Fatalf("synthetic capacity revision range is exhausted: first=%d count=%d", firstSyntheticRevision, seed.Count)
	}
	if _, err := tx.ExecContext(t.Context(), `
		CREATE TEMP TABLE writer_capacity_receipt_seed (
			request_id TEXT NOT NULL COLLATE BINARY PRIMARY KEY,
			catalog_revision INTEGER NOT NULL,
			catalog_state_token BLOB NOT NULL CHECK (length(catalog_state_token) = 32),
			outcome_proto BLOB NOT NULL
		) STRICT, WITHOUT ROWID`); err != nil {
		t.Fatalf("create synthetic capacity staging table: %v", err)
	}
	type stagedCapacityReceipt struct {
		requestID string
		revision  int64
		token     []byte
		outcome   []byte
	}
	staged := make([]stagedCapacityReceipt, 0, seed.Count)
	for index := 0; index < seed.Count; index++ {
		requestID := fmt.Sprintf("%s%08d", seed.RequestIDPrefix, index)
		revision := firstSyntheticRevision + int64(index)
		token := sha256.Sum256([]byte(fmt.Sprintf(
			"open-splunk-capacity-fixture/%s/%d",
			requestID,
			revision,
		)))
		copyEnvelope := proto.Clone(envelope).(*opensplunkv1.KnowledgeMutationOutcomeRecord)
		copyEnvelope.TenantCatalogRevision = uint64(revision)
		copyEnvelope.TenantCatalogStateToken = bytes.Clone(token[:])
		copyOutcome, err := (proto.MarshalOptions{Deterministic: true}).Marshal(copyEnvelope)
		if err != nil {
			t.Fatalf("encode synthetic capacity outcome %d: %v", index, err)
		}
		staged = append(staged, stagedCapacityReceipt{
			requestID: requestID,
			revision:  revision,
			token:     bytes.Clone(token[:]),
			outcome:   copyOutcome,
		})
	}
	const capacitySeedBatch = 256
	for offset := 0; offset < len(staged); offset += capacitySeedBatch {
		end := min(offset+capacitySeedBatch, len(staged))
		placeholders := strings.TrimSuffix(strings.Repeat("(?, ?, ?, ?),", end-offset), ",")
		arguments := make([]any, 0, (end-offset)*4)
		for _, record := range staged[offset:end] {
			arguments = append(arguments, record.requestID, record.revision, record.token, record.outcome)
		}
		// #nosec G202 -- placeholders is generated only from a bounded batch size and contains no data.
		if _, err := tx.ExecContext(t.Context(), `
			INSERT INTO writer_capacity_receipt_seed (
				request_id, catalog_revision, catalog_state_token, outcome_proto
			) VALUES `+placeholders, arguments...); err != nil {
			t.Fatalf("stage synthetic capacity receipts %d..%d: %v", offset, end, err)
		}
	}
	commitResult, err := tx.ExecContext(t.Context(), `
		INSERT INTO knowledge_mutation_commit_authorities (
			tenant_id, actor_kind, actor_id, route, client_request_id,
			request_digest, catalog_revision, catalog_state_token,
			mutation_kind, knowledge_object_id, object_version,
			occurred_at_unix_micro, retention_anchor_unix_micro,
			retain_until_unix_micro, successful_audit_sequence,
			recovery_audit_sequence
		)
		SELECT source.tenant_id, source.actor_kind, source.actor_id, source.route,
		       seed.request_id, source.request_digest, seed.catalog_revision,
		       seed.catalog_state_token, source.mutation_kind,
		       source.knowledge_object_id, source.object_version,
		       source.occurred_at_unix_micro, source.retention_anchor_unix_micro,
		       source.retain_until_unix_micro, source.successful_audit_sequence,
		       source.recovery_audit_sequence
		FROM knowledge_mutation_commit_authorities AS source
		CROSS JOIN writer_capacity_receipt_seed AS seed
		WHERE source.tenant_id = ? AND source.catalog_revision = ?`,
		writerTestTenant,
		sourceRevision,
	)
	if err != nil {
		t.Fatalf("seed %d synthetic commit authorities: %v", seed.Count, err)
	}
	if affected, affectedErr := commitResult.RowsAffected(); affectedErr != nil || affected != int64(seed.Count) {
		t.Fatalf("synthetic commit authorities affected %d, want %d: %v", affected, seed.Count, affectedErr)
	}
	receiptResult, err := tx.ExecContext(t.Context(), `
		INSERT INTO knowledge_mutation_idempotency (
			tenant_id, actor_kind, actor_id, route, client_request_id,
			mutation_kind, request_digest_format_version, request_digest,
			outcome_format_version, outcome_proto,
			committed_catalog_revision, committed_catalog_state_token,
			knowledge_object_id, object_version,
			successful_audit_sequence, recovery_audit_sequence,
			created_at_unix_micro, retention_anchor_unix_micro,
			retain_until_unix_micro
		)
		SELECT source.tenant_id, source.actor_kind, source.actor_id, source.route,
		       seed.request_id, source.mutation_kind,
		       source.request_digest_format_version, source.request_digest,
		       source.outcome_format_version, seed.outcome_proto,
		       seed.catalog_revision, seed.catalog_state_token,
		       source.knowledge_object_id, source.object_version,
		       source.successful_audit_sequence, source.recovery_audit_sequence,
		       source.created_at_unix_micro, source.retention_anchor_unix_micro,
		       source.retain_until_unix_micro
		FROM knowledge_mutation_idempotency AS source
		CROSS JOIN writer_capacity_receipt_seed AS seed
		WHERE source.tenant_id = ? AND source.actor_kind = 'browser'
		  AND source.actor_id = 'writer-blackbox-administrator'
		  AND source.route = 'objects.create' AND source.client_request_id = ?`,
		writerTestTenant,
		seed.SourceRequestID,
	)
	if err != nil {
		t.Fatalf("seed %d synthetic capacity receipts: %v", seed.Count, err)
	}
	if affected, affectedErr := receiptResult.RowsAffected(); affectedErr != nil || affected != int64(seed.Count) {
		t.Fatalf("synthetic capacity receipts affected %d, want %d: %v", affected, seed.Count, affectedErr)
	}
	if _, err := tx.ExecContext(t.Context(), `DROP TABLE writer_capacity_receipt_seed`); err != nil {
		t.Fatalf("drop synthetic capacity staging table: %v", err)
	}
	if _, err := tx.ExecContext(t.Context(), `
		UPDATE knowledge_catalog_tenants
		SET idempotency_count = (
			SELECT count(*) FROM knowledge_mutation_idempotency
			WHERE tenant_id = ?
		)
		WHERE tenant_id = ?`, writerTestTenant, writerTestTenant); err != nil {
		t.Fatalf("close synthetic capacity ledger: %v", err)
	}
	for index, statement := range receiptInsertTriggerSQL {
		if _, err := tx.ExecContext(t.Context(), statement); err != nil {
			t.Fatalf("restore %s trigger: %v", receiptInsertTriggerNames[index], err)
		}
	}
	if _, err := tx.ExecContext(t.Context(), receiptUpdateTriggerSQL); err != nil {
		t.Fatalf("restore receipt immutability trigger: %v", err)
	}
	if _, err := tx.ExecContext(t.Context(), commitUpdateTriggerSQL); err != nil {
		t.Fatalf("restore commit-authority immutability trigger: %v", err)
	}
	if _, err := tx.ExecContext(t.Context(), commitCollisionTriggerSQL); err != nil {
		t.Fatalf("restore commit-authority collision trigger: %v", err)
	}
	if _, err := tx.ExecContext(t.Context(), commitExactTriggerSQL); err != nil {
		t.Fatalf("restore commit-authority exactness trigger: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit capacity receipt seed: %v", err)
	}
}

func newCapacityTestWriter(
	t *testing.T,
	harness *writerBlackboxHarness,
	now time.Time,
	retention time.Duration,
	objectIDPrefix string,
) (*knowledgecatalog.Writer, *capacityWriterCalls) {
	t.Helper()
	calls := &capacityWriterCalls{}
	writer, err := knowledgecatalog.NewWriter(harness.database, harness.audit, knowledgecatalog.WriterOptions{
		Clock: func() time.Time {
			call := calls.Clocks.Add(1)
			return now.Add(time.Duration(call-1) * time.Microsecond)
		},
		IDGenerator: func() (string, error) {
			return fmt.Sprintf("%s_%06d", objectIDPrefix, calls.IDs.Add(1)), nil
		},
		IdempotencyRetention: retention,
	})
	if err != nil {
		t.Fatalf("knowledgecatalog.NewWriter(capacity fixture): %v", err)
	}
	return writer, calls
}

type capacityWriterCalls struct {
	IDs    atomic.Int64
	Clocks atomic.Int64
}

func capacityCreateRequest(name string, requestID string) *opensplunkv1.CreateKnowledgeObjectRequest {
	description := "capacity definition for " + name
	return &opensplunkv1.CreateKnowledgeObjectRequest{
		Definition: writerAliasDefinition(
			writerTestApp,
			name,
			&description,
			opensplunkv1.SharingScope_SHARING_SCOPE_PRIVATE,
			"host-"+name,
			"source_field",
			"destination_"+name,
		),
		InitialState:    opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DRAFT,
		ClientRequestId: requestID,
	}
}

func assertCapacityRejectedWithoutPublication(
	t *testing.T,
	harness *writerBlackboxHarness,
	writer *knowledgecatalog.Writer,
	request *opensplunkv1.CreateKnowledgeObjectRequest,
	want writerAuthoritySnapshot,
) {
	t.Helper()
	if _, err := writer.Create(harness.actorCtx, harness.writeScope, request); !errors.Is(err, control.ErrCapacityExceeded) {
		t.Fatalf("Create() at unexpired normal capacity error = %v, want ErrCapacityExceeded", err)
	}
	assertWriterAuthoritySnapshotsEqual(t, readWriterAuthoritySnapshot(t, harness.database), want)
}

func assertCapacityReceiptExists(t *testing.T, database *control.DB, requestID string) {
	t.Helper()
	var count int
	if err := database.SQLDB().QueryRowContext(t.Context(), `
		SELECT count(*)
		FROM knowledge_mutation_idempotency
		WHERE tenant_id = ? AND actor_kind = 'browser'
		  AND actor_id = 'writer-blackbox-administrator'
		  AND route = 'objects.create' AND client_request_id = ?`,
		writerTestTenant,
		requestID,
	).Scan(&count); err != nil {
		t.Fatalf("count idempotency receipt %q: %v", requestID, err)
	}
	if count != 1 {
		t.Fatalf("idempotency receipt %q count = %d, want 1", requestID, count)
	}
}

func assertCapacityReceiptRouteCount(
	t *testing.T,
	database *control.DB,
	route string,
	requestID string,
	want int,
) {
	t.Helper()
	var got int
	if err := database.SQLDB().QueryRowContext(t.Context(), `
		SELECT count(*)
		FROM knowledge_mutation_idempotency
		WHERE tenant_id = ? AND route = ? AND client_request_id = ?`,
		writerTestTenant,
		route,
		requestID,
	).Scan(&got); err != nil {
		t.Fatalf("count idempotency receipt %s/%q: %v", route, requestID, err)
	}
	if got != want {
		t.Fatalf("idempotency receipt %s/%q count = %d, want %d", route, requestID, got, want)
	}
}

func assertExpiredCapacityPrefixReclaimed(
	t *testing.T,
	database *control.DB,
	deleted int,
	total int,
) {
	t.Helper()
	assertCapacityReceiptSuffixReclaimed(
		t,
		database,
		"expired-capacity-000-filler-",
		deleted,
		total,
	)
}

func assertCapacityReceiptSuffixReclaimed(
	t *testing.T,
	database *control.DB,
	prefix string,
	deleted int,
	total int,
) {
	t.Helper()
	rows, err := database.SQLDB().QueryContext(t.Context(), `
		SELECT client_request_id
		FROM knowledge_mutation_idempotency
		WHERE tenant_id = ? AND client_request_id GLOB ?
		ORDER BY client_request_id`, writerTestTenant, prefix+"*")
	if err != nil {
		t.Fatalf("read surviving capacity receipts for %q: %v", prefix, err)
	}
	defer rows.Close()
	wantIndex := deleted
	for rows.Next() {
		var requestID string
		if err := rows.Scan(&requestID); err != nil {
			t.Fatalf("scan surviving capacity receipt for %q: %v", prefix, err)
		}
		want := fmt.Sprintf("%s%08d", prefix, wantIndex)
		if requestID != want {
			t.Fatalf("surviving capacity receipt = %q, want oldest-prefix survivor %q", requestID, want)
		}
		wantIndex++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate surviving capacity receipts for %q: %v", prefix, err)
	}
	if wantIndex != total {
		t.Fatalf("surviving capacity receipt suffix for %q ended at %d, want %d", prefix, wantIndex, total)
	}
}

func assertReceiptPrefixCount(t *testing.T, database *control.DB, prefix string, want int) {
	t.Helper()
	var got int
	if err := database.SQLDB().QueryRowContext(t.Context(), `
		SELECT count(*)
		FROM knowledge_mutation_idempotency
		WHERE tenant_id = ? AND client_request_id GLOB ?`, writerTestTenant, prefix+"*").Scan(&got); err != nil {
		t.Fatalf("count capacity receipt prefix %q: %v", prefix, err)
	}
	if got != want {
		t.Fatalf("capacity receipt prefix %q count = %d, want %d", prefix, got, want)
	}
}

func capacityReceiptRetention(t *testing.T, database *control.DB, requestID string) time.Duration {
	t.Helper()
	var retentionMicros int64
	if err := database.SQLDB().QueryRowContext(t.Context(), `
		SELECT retain_until_unix_micro - retention_anchor_unix_micro
		FROM knowledge_mutation_idempotency
		WHERE tenant_id = ? AND actor_kind = 'browser'
		  AND actor_id = 'writer-blackbox-administrator'
		  AND route = 'objects.create' AND client_request_id = ?`,
		writerTestTenant,
		requestID,
	).Scan(&retentionMicros); err != nil {
		t.Fatalf("read idempotency retention for %q: %v", requestID, err)
	}
	return time.Duration(retentionMicros) * time.Microsecond
}

func readCapacityReceiptActorBytes(t *testing.T, database *control.DB, requestID string) []byte {
	t.Helper()
	var actor []byte
	if err := database.SQLDB().QueryRowContext(t.Context(), `
		SELECT CAST(actor_id AS BLOB)
		FROM knowledge_mutation_idempotency
		WHERE tenant_id = ? AND route = 'objects.create' AND client_request_id = ?`,
		writerTestTenant,
		requestID,
	).Scan(&actor); err != nil {
		t.Fatalf("read capacity receipt actor for %q: %v", requestID, err)
	}
	return bytes.Clone(actor)
}

type capacityReceiptKey struct {
	ActorKind       string
	ActorID         string
	Route           string
	ClientRequestID string
	MutationKind    string
}

func readOldestCapacityReceiptKey(t *testing.T, database *control.DB) capacityReceiptKey {
	t.Helper()
	var key capacityReceiptKey
	if err := database.SQLDB().QueryRowContext(t.Context(), `
		SELECT actor_kind, actor_id, route, client_request_id, mutation_kind
		FROM knowledge_mutation_idempotency
		WHERE tenant_id = ? AND retain_until_unix_micro <=
		      CAST(unixepoch('subsec') * 1000000 AS INTEGER)
		ORDER BY retain_until_unix_micro,
		         created_at_unix_micro,
		         actor_kind,
		         actor_id,
		         route,
		         client_request_id
		LIMIT 1`, writerTestTenant).Scan(
		&key.ActorKind,
		&key.ActorID,
		&key.Route,
		&key.ClientRequestID,
		&key.MutationKind,
	); err != nil {
		t.Fatalf("read oldest capacity receipt key: %v", err)
	}
	return key
}

type capacityContextExecer interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

func assertCapacityForeignKeys(t *testing.T, connection *sql.Conn, want int) {
	t.Helper()
	var got int
	if err := connection.QueryRowContext(t.Context(), `PRAGMA foreign_keys`).Scan(&got); err != nil {
		t.Fatalf("read foreign-key state: %v", err)
	}
	if got != want {
		t.Fatalf("foreign-key state = %d, want %d", got, want)
	}
}

func assertOneCapacityRowUpdated(t *testing.T, connection capacityContextExecer, query string, args ...any) {
	t.Helper()
	result, err := connection.ExecContext(t.Context(), query, args...)
	if err != nil {
		t.Fatalf("update capacity corruption fixture: %v", err)
	}
	affected, err := result.RowsAffected()
	if err != nil || affected != 1 {
		t.Fatalf("capacity corruption fixture rows = %d, %v, want 1", affected, err)
	}
}

func explainCapacityQueryPlan(t *testing.T, database *sql.DB, query string, args ...any) string {
	t.Helper()
	rows, err := database.QueryContext(t.Context(), "EXPLAIN QUERY PLAN "+query, args...)
	if err != nil {
		t.Fatalf("explain capacity query plan: %v", err)
	}
	defer rows.Close()
	var details []string
	for rows.Next() {
		var id, parent, unused int
		var detail string
		if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
			t.Fatalf("scan capacity query plan: %v", err)
		}
		details = append(details, detail)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate capacity query plan: %v", err)
	}
	return strings.ToUpper(strings.Join(details, "\n"))
}
