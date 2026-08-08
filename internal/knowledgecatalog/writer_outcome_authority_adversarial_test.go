package knowledgecatalog

import (
	"bytes"
	"errors"
	"strings"
	"testing"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/audit"
	"google.golang.org/protobuf/proto"
)

func TestWriterReplayRejectsTamperedCompactOutcomeAuthorities(t *testing.T) {
	tests := []struct {
		name   string
		slug   string
		tamper func(*testing.T, *writerFaultHarness, string, *opensplunkv1.KnowledgeMutationOutcomeRecord)
	}{
		{
			name: "outcome state token",
			slug: "outcome-state-token",
			tamper: func(t *testing.T, harness *writerFaultHarness, requestID string, envelope *opensplunkv1.KnowledgeMutationOutcomeRecord) {
				t.Helper()
				envelope.TenantCatalogStateToken = bytes.Repeat([]byte{0x92}, catalogStateTokenBytes)
				updateWriterOutcomeReceipt(t, harness, requestID,
					`outcome_proto = ?`, marshalWriterOutcomeEnvelope(t, envelope))
			},
		},
		{
			name: "outcome revision",
			slug: "outcome-revision",
			tamper: func(t *testing.T, harness *writerFaultHarness, requestID string, envelope *opensplunkv1.KnowledgeMutationOutcomeRecord) {
				t.Helper()
				envelope.TenantCatalogRevision++
				updateWriterOutcomeReceipt(t, harness, requestID,
					`outcome_proto = ?`, marshalWriterOutcomeEnvelope(t, envelope))
			},
		},
		{
			name: "outcome immutable digest",
			slug: "outcome-immutable-digest",
			tamper: func(t *testing.T, harness *writerFaultHarness, requestID string, envelope *opensplunkv1.KnowledgeMutationOutcomeRecord) {
				t.Helper()
				envelope.GetObject().DefinitionSha256 = bytes.Clone(envelope.GetObject().GetDefinitionSha256())
				envelope.GetObject().DefinitionSha256[0] ^= 0xff
				updateWriterOutcomeReceipt(t, harness, requestID,
					`outcome_proto = ?`, marshalWriterOutcomeEnvelope(t, envelope))
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			harness := newWriterFaultHarness(t)
			request := writerFaultCreateRequest("outcome-authority-"+test.slug, "outcome-authority-request-0001")
			if _, err := harness.writer.Create(harness.actorContext, harness.scope, request); err != nil {
				t.Fatalf("commit Create(): %v", err)
			}
			dropWriterOutcomeReceiptUpdateGuard(t, harness)
			envelope := readWriterOutcomeEnvelope(t, harness, request.GetClientRequestId())
			test.tamper(t, harness, request.GetClientRequestId(), envelope)
			stable := readWriterFaultSnapshot(t, harness.database)

			response, err := harness.writer.Create(
				harness.actorContext,
				harness.scope,
				proto.Clone(request).(*opensplunkv1.CreateKnowledgeObjectRequest),
			)
			if response != nil || !errors.Is(err, ErrCorrupt) {
				t.Fatalf("Create() with tampered outcome authority = (%v, %v), want nil/ErrCorrupt", response, err)
			}
			assertWriterFaultSnapshotsEqual(t, readWriterFaultSnapshot(t, harness.database), stable)
			assertWriterFaultIntegrity(t, harness.database)
		})
	}
}

func TestWriterReceiptSnapshotPairIsForeignKeyBoundAndIndependentlyReplayVerified(t *testing.T) {
	harness := newWriterFaultHarness(t)
	request := writerFaultCreateRequest("outcome-snapshot-pair", "outcome-snapshot-pair-0001")
	committed, err := harness.writer.Create(harness.actorContext, harness.scope, request)
	if err != nil {
		t.Fatalf("commit Create(): %v", err)
	}
	dropWriterOutcomeReceiptUpdateGuard(t, harness)
	envelope := readWriterOutcomeEnvelope(t, harness, request.GetClientRequestId())
	originalRevision := envelope.GetTenantCatalogRevision()
	originalToken := bytes.Clone(envelope.GetTenantCatalogStateToken())
	originalOutcome := marshalWriterOutcomeEnvelope(t, envelope)

	if _, err := harness.database.SQLDB().ExecContext(t.Context(), `
		UPDATE knowledge_mutation_idempotency
		SET committed_catalog_state_token = ?
		WHERE tenant_id = ? AND route = ? AND client_request_id = ?`,
		bytes.Repeat([]byte{0x91}, catalogStateTokenBytes),
		writerFaultTenant,
		mutationRouteCreate,
		request.GetClientRequestId(),
	); err == nil || !strings.Contains(err.Error(), "FOREIGN KEY") {
		t.Fatalf("row-only state-token tamper error = %v, want foreign-key rejection", err)
	}
	replayed, err := harness.writer.Create(
		harness.actorContext,
		harness.scope,
		proto.Clone(request).(*opensplunkv1.CreateKnowledgeObjectRequest),
	)
	if err != nil || !proto.Equal(replayed, committed) {
		t.Fatalf("replay after rejected row tamper = (%v, %v), want %v", replayed, err, committed)
	}

	forgedRevision := originalRevision + 7
	forgedToken := bytes.Repeat([]byte{0x92}, catalogStateTokenBytes)
	envelope.TenantCatalogRevision = forgedRevision
	envelope.TenantCatalogStateToken = bytes.Clone(forgedToken)
	updateWriterOutcomeReceiptWithoutForeignKeys(
		t,
		harness,
		request.GetClientRequestId(),
		`committed_catalog_revision = ?, committed_catalog_state_token = ?, outcome_proto = ?`,
		forgedRevision,
		forgedToken,
		marshalWriterOutcomeEnvelope(t, envelope),
	)
	stable := readWriterFaultSnapshot(t, harness.database)
	response, err := harness.writer.Create(
		harness.actorContext,
		harness.scope,
		proto.Clone(request).(*opensplunkv1.CreateKnowledgeObjectRequest),
	)
	if response != nil || !errors.Is(err, ErrCorrupt) {
		t.Fatalf("replay with coordinated forged snapshot pair = (%v, %v), want nil/ErrCorrupt", response, err)
	}
	assertWriterFaultSnapshotsEqual(t, readWriterFaultSnapshot(t, harness.database), stable)

	updateWriterOutcomeReceiptWithoutForeignKeys(
		t,
		harness,
		request.GetClientRequestId(),
		`committed_catalog_revision = ?, committed_catalog_state_token = ?, outcome_proto = ?`,
		originalRevision,
		originalToken,
		originalOutcome,
	)
	assertWriterFaultIntegrity(t, harness.database)
}

func TestWriterReplayRejectsCoordinatedOutcomeAndRowAuditRebinding(t *testing.T) {
	harness := newWriterFaultHarness(t)
	first := writerFaultCreateRequest("outcome-audit-first", "outcome-audit-first-0001")
	created, err := harness.writer.Create(harness.actorContext, harness.scope, first)
	if err != nil {
		t.Fatalf("commit first Create(): %v", err)
	}
	var occurredAt int64
	if err := harness.database.SQLDB().QueryRowContext(t.Context(), `
		SELECT created_at_unix_micro FROM knowledge_mutation_idempotency
		WHERE tenant_id = ? AND route = ? AND client_request_id = ?`,
		writerFaultTenant, mutationRouteCreate, first.GetClientRequestId(),
	).Scan(&occurredAt); err != nil {
		t.Fatalf("read first audit occurrence: %v", err)
	}
	auditStore, err := audit.NewStore(harness.database, audit.StoreOptions{})
	if err != nil {
		t.Fatalf("audit.NewStore(): %v", err)
	}
	duplicate, err := auditStore.Append(harness.actorContext, writerFaultTenant, audit.SuccessfulEvent{
		OccurredAt:    time.UnixMicro(occurredAt).UTC(),
		Action:        audit.ActionKnowledgeObjectCreate,
		TargetKind:    audit.TargetKindKnowledgeObject,
		TargetID:      created.GetKnowledgeObject().GetKnowledgeObjectId(),
		TargetVersion: 1,
		KnowledgeObject: audit.KnowledgeObjectMetadata{
			AppID:        writerFaultApp,
			ObjectType:   audit.KnowledgeObjectTypeFieldAlias,
			SharingScope: audit.KnowledgeSharingScopePrivate,
		},
	})
	if err != nil {
		t.Fatalf("append byte-equivalent duplicate audit: %v", err)
	}

	dropWriterOutcomeReceiptUpdateGuard(t, harness)
	envelope := readWriterOutcomeEnvelope(t, harness, first.GetClientRequestId())
	originalAuditSequence := envelope.GetSuccessfulAuditSequence()
	originalOutcome := marshalWriterOutcomeEnvelope(t, envelope)
	envelope.AuditAuthority = &opensplunkv1.KnowledgeMutationOutcomeRecord_SuccessfulAuditSequence{
		SuccessfulAuditSequence: duplicate.Sequence,
	}
	updateWriterOutcomeReceiptWithoutForeignKeys(
		t,
		harness,
		first.GetClientRequestId(),
		`successful_audit_sequence = ?, outcome_proto = ?`,
		int64(duplicate.Sequence),
		marshalWriterOutcomeEnvelope(t, envelope),
	)
	stable := readWriterFaultSnapshot(t, harness.database)
	response, err := harness.writer.Create(
		harness.actorContext,
		harness.scope,
		proto.Clone(first).(*opensplunkv1.CreateKnowledgeObjectRequest),
	)
	if response != nil || !errors.Is(err, ErrCorrupt) {
		t.Fatalf("Create() with rebound audit authority = (%v, %v), want nil/ErrCorrupt", response, err)
	}
	assertWriterFaultSnapshotsEqual(t, readWriterFaultSnapshot(t, harness.database), stable)
	updateWriterOutcomeReceiptWithoutForeignKeys(
		t,
		harness,
		first.GetClientRequestId(),
		`successful_audit_sequence = ?, outcome_proto = ?`,
		int64(originalAuditSequence),
		originalOutcome,
	)
	assertWriterFaultIntegrity(t, harness.database)
}

func TestWriterReplayRejectsWholeReceiptRebindingAcrossEquivalentCreates(t *testing.T) {
	harness := newWriterFaultHarness(t)
	first := writerFaultCreateRequest("whole-receipt-equivalent", "whole-receipt-first-0001")
	if _, err := harness.writer.Create(harness.actorContext, harness.scope, first); err != nil {
		t.Fatalf("commit first equivalent Create(): %v", err)
	}
	second := writerFaultCreateRequest("whole-receipt-equivalent", "whole-receipt-second-0001")
	if _, err := harness.writer.Create(harness.actorContext, harness.scope, second); err != nil {
		t.Fatalf("commit second equivalent Create(): %v", err)
	}

	type receiptAuthorities struct {
		MutationKind            string
		Outcome                 []byte
		Revision                int64
		Token                   []byte
		ObjectID                string
		Version                 int64
		SuccessfulAuditSequence int64
		CreatedAt               int64
		RetentionAnchor         int64
		RetainUntil             int64
	}
	read := func(requestID string) receiptAuthorities {
		var authority receiptAuthorities
		if err := harness.database.SQLDB().QueryRowContext(t.Context(), `SELECT
			mutation_kind, outcome_proto, committed_catalog_revision,
			committed_catalog_state_token, knowledge_object_id, object_version,
			successful_audit_sequence, created_at_unix_micro,
			retention_anchor_unix_micro, retain_until_unix_micro
		FROM knowledge_mutation_idempotency
		WHERE tenant_id = ? AND actor_kind = ? AND actor_id = ?
		  AND route = ? AND client_request_id = ?`,
			writerFaultTenant,
			audit.ActorKindBrowser,
			"writer-fault-administrator",
			mutationRouteCreate,
			requestID,
		).Scan(
			&authority.MutationKind,
			&authority.Outcome,
			&authority.Revision,
			&authority.Token,
			&authority.ObjectID,
			&authority.Version,
			&authority.SuccessfulAuditSequence,
			&authority.CreatedAt,
			&authority.RetentionAnchor,
			&authority.RetainUntil,
		); err != nil {
			t.Fatalf("read %s equivalent receipt: %v", requestID, err)
		}
		return authority
	}
	original := read(first.GetClientRequestId())
	winner := read(second.GetClientRequestId())
	if original.ObjectID == winner.ObjectID || original.Revision == winner.Revision ||
		bytes.Equal(original.Token, winner.Token) {
		t.Fatalf("equivalent Create authorities unexpectedly agree: first=%#v second=%#v", original, winner)
	}

	dropWriterOutcomeReceiptUpdateGuard(t, harness)
	assignment := `mutation_kind = ?, outcome_proto = ?, committed_catalog_revision = ?,
		committed_catalog_state_token = ?, knowledge_object_id = ?, object_version = ?,
		successful_audit_sequence = ?, created_at_unix_micro = ?,
		retention_anchor_unix_micro = ?, retain_until_unix_micro = ?`
	values := []any{
		winner.MutationKind, winner.Outcome, winner.Revision, winner.Token,
		winner.ObjectID, winner.Version, winner.SuccessfulAuditSequence,
		winner.CreatedAt, winner.RetentionAnchor, winner.RetainUntil,
	}
	if _, err := harness.database.SQLDB().ExecContext(t.Context(), `
		UPDATE knowledge_mutation_idempotency SET `+assignment+`
		WHERE tenant_id = ? AND actor_kind = ? AND actor_id = ?
		  AND route = ? AND client_request_id = ?`, append(values,
		writerFaultTenant, audit.ActorKindBrowser, "writer-fault-administrator",
		mutationRouteCreate, first.GetClientRequestId())...); err == nil {
		t.Fatal("whole receipt rebinding succeeded with composite foreign key enabled")
	}
	updateWriterOutcomeReceiptWithoutForeignKeys(
		t, harness, first.GetClientRequestId(), assignment, values...,
	)
	stable := readWriterFaultSnapshot(t, harness.database)
	response, err := harness.writer.Create(
		harness.actorContext,
		harness.scope,
		proto.Clone(first).(*opensplunkv1.CreateKnowledgeObjectRequest),
	)
	if response != nil || !errors.Is(err, ErrCorrupt) {
		t.Fatalf("Create() with whole rebound receipt = (%v, %v), want nil/ErrCorrupt", response, err)
	}
	assertWriterFaultSnapshotsEqual(t, readWriterFaultSnapshot(t, harness.database), stable)
	updateWriterOutcomeReceiptWithoutForeignKeys(
		t,
		harness,
		first.GetClientRequestId(),
		assignment,
		original.MutationKind,
		original.Outcome,
		original.Revision,
		original.Token,
		original.ObjectID,
		original.Version,
		original.SuccessfulAuditSequence,
		original.CreatedAt,
		original.RetentionAnchor,
		original.RetainUntil,
	)
	assertWriterFaultIntegrity(t, harness.database)
}

func dropWriterOutcomeReceiptUpdateGuard(t *testing.T, harness *writerFaultHarness) {
	t.Helper()
	if _, err := harness.database.SQLDB().ExecContext(t.Context(),
		`DROP TRIGGER knowledge_mutation_idempotency_update_is_forbidden`); err != nil {
		t.Fatalf("drop immutable receipt update trigger: %v", err)
	}
}

func readWriterOutcomeEnvelope(
	t *testing.T,
	harness *writerFaultHarness,
	requestID string,
) *opensplunkv1.KnowledgeMutationOutcomeRecord {
	t.Helper()
	var encoded []byte
	if err := harness.database.SQLDB().QueryRowContext(t.Context(), `SELECT outcome_proto
		FROM knowledge_mutation_idempotency
		WHERE tenant_id = ? AND actor_kind = ? AND actor_id = ?
		  AND route = ? AND client_request_id = ?`,
		writerFaultTenant,
		audit.ActorKindBrowser,
		"writer-fault-administrator",
		mutationRouteCreate,
		requestID,
	).Scan(&encoded); err != nil {
		t.Fatalf("read compact outcome: %v", err)
	}
	envelope := &opensplunkv1.KnowledgeMutationOutcomeRecord{}
	if err := (proto.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(encoded, envelope); err != nil ||
		len(envelope.ProtoReflect().GetUnknown()) != 0 || envelope.GetObject() == nil {
		t.Fatalf("decode compact outcome = (%v, %v)", envelope, err)
	}
	return envelope
}

func marshalWriterOutcomeEnvelope(
	t *testing.T,
	envelope *opensplunkv1.KnowledgeMutationOutcomeRecord,
) []byte {
	t.Helper()
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(envelope)
	if err != nil || len(encoded) < 1 || len(encoded) > maximumMutationOutcomeBytes {
		t.Fatalf("marshal compact outcome = %d bytes, %v", len(encoded), err)
	}
	return encoded
}

func updateWriterOutcomeReceipt(
	t *testing.T,
	harness *writerFaultHarness,
	requestID string,
	assignment string,
	values ...any,
) {
	t.Helper()
	arguments := append(values,
		writerFaultTenant,
		audit.ActorKindBrowser,
		"writer-fault-administrator",
		mutationRouteCreate,
		requestID,
	)
	result, err := harness.database.SQLDB().ExecContext(t.Context(), `UPDATE knowledge_mutation_idempotency
		SET `+assignment+`
		WHERE tenant_id = ? AND actor_kind = ? AND actor_id = ?
		  AND route = ? AND client_request_id = ?`, arguments...)
	if err != nil {
		t.Fatalf("tamper compact outcome: %v", err)
	}
	if affected, err := result.RowsAffected(); err != nil || affected != 1 {
		t.Fatalf("tamper compact outcome rows = %d, %v; want 1", affected, err)
	}
}

func updateWriterOutcomeReceiptWithoutForeignKeys(
	t *testing.T,
	harness *writerFaultHarness,
	requestID string,
	assignment string,
	values ...any,
) {
	t.Helper()
	connection, err := harness.database.SQLDB().Conn(t.Context())
	if err != nil {
		t.Fatalf("open receipt-tamper connection: %v", err)
	}
	defer connection.Close()
	if _, err := connection.ExecContext(t.Context(), `PRAGMA foreign_keys = OFF`); err != nil {
		t.Fatalf("disable receipt foreign keys: %v", err)
	}
	arguments := append(values,
		writerFaultTenant,
		audit.ActorKindBrowser,
		"writer-fault-administrator",
		mutationRouteCreate,
		requestID,
	)
	result, updateErr := connection.ExecContext(t.Context(), `UPDATE knowledge_mutation_idempotency
		SET `+assignment+`
		WHERE tenant_id = ? AND actor_kind = ? AND actor_id = ?
		  AND route = ? AND client_request_id = ?`, arguments...)
	if _, err := connection.ExecContext(t.Context(), `PRAGMA foreign_keys = ON`); err != nil {
		t.Fatalf("restore receipt foreign keys: %v", err)
	}
	if updateErr != nil {
		t.Fatalf("tamper compact outcome without foreign keys: %v", updateErr)
	}
	if affected, err := result.RowsAffected(); err != nil || affected != 1 {
		t.Fatalf("tamper compact outcome rows = %d, %v; want 1", affected, err)
	}
}
