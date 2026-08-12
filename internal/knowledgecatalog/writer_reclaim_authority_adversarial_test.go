package knowledgecatalog_test

import (
	"bytes"
	"database/sql"
	"errors"
	"testing"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/knowledgecatalog"
	"google.golang.org/protobuf/proto"
)

func TestWriterIdempotencyReclaimRejectsCorruptVersionLifecycleAndAuditAuthorities(t *testing.T) {
	tests := []struct {
		name    string
		tamper  func(*testing.T, *writerReclaimAuthorityFixture)
		restore func(*testing.T, *writerReclaimAuthorityFixture)
	}{
		{
			name: "canonical outcome definition digest",
			tamper: func(t *testing.T, fixture *writerReclaimAuthorityFixture) {
				fixture.dropTrigger(t, "knowledge_mutation_idempotency_update_is_forbidden")
				var encoded []byte
				if err := fixture.connection.QueryRowContext(t.Context(), `
					SELECT outcome_proto FROM knowledge_mutation_idempotency
					WHERE tenant_id = ? AND client_request_id = ?`,
					writerTestTenant, fixture.corruptRequestID,
				).Scan(&encoded); err != nil {
					t.Fatalf("read reclaim outcome: %v", err)
				}
				fixture.originalOutcome = bytes.Clone(encoded)
				envelope := &opensplunkv1.KnowledgeMutationOutcomeRecord{}
				if err := proto.Unmarshal(encoded, envelope); err != nil ||
					len(envelope.GetObject().GetDefinitionSha256()) != 32 {
					t.Fatalf("decode reclaim outcome = (%v, %v)", envelope, err)
				}
				envelope.GetObject().DefinitionSha256[0] ^= 0xff
				corrupt, err := (proto.MarshalOptions{Deterministic: true}).Marshal(envelope)
				if err != nil {
					t.Fatalf("encode corrupt reclaim outcome: %v", err)
				}
				fixture.updateReceipt(t, "outcome_proto = ?", corrupt)
			},
			restore: func(t *testing.T, fixture *writerReclaimAuthorityFixture) {
				fixture.updateReceipt(t, "outcome_proto = ?", fixture.originalOutcome)
			},
		},
		{
			name: "immutable lifecycle marker",
			tamper: func(t *testing.T, fixture *writerReclaimAuthorityFixture) {
				fixture.dropTrigger(t, "knowledge_object_version_lifecycle_update_is_forbidden")
				fixture.setIgnoreChecks(t, true)
				assertOneCapacityRowUpdated(t, fixture.connection, `
					UPDATE knowledge_object_version_lifecycle
					SET disabled_at_unix_micro = ?
					WHERE tenant_id = ? AND knowledge_object_id = ? AND object_version = ?`,
					fixture.createdAt, writerTestTenant, fixture.objectID, fixture.objectVersion,
				)
				fixture.setIgnoreChecks(t, false)
			},
			restore: func(t *testing.T, fixture *writerReclaimAuthorityFixture) {
				assertOneCapacityRowUpdated(t, fixture.connection, `
					UPDATE knowledge_object_version_lifecycle
					SET disabled_at_unix_micro = NULL
					WHERE tenant_id = ? AND knowledge_object_id = ? AND object_version = ?`,
					writerTestTenant, fixture.objectID, fixture.objectVersion,
				)
			},
		},
		{
			name: "successful audit actor role",
			tamper: func(t *testing.T, fixture *writerReclaimAuthorityFixture) {
				fixture.dropTrigger(t, "audit_event_update_is_forbidden")
				fixture.setIgnoreChecks(t, true)
				assertOneCapacityRowUpdated(t, fixture.connection, `
					UPDATE audit_events SET actor_role = 'user'
					WHERE tenant_id = ? AND sequence = ?`, writerTestTenant, fixture.auditSequence)
				fixture.setIgnoreChecks(t, false)
			},
			restore: func(t *testing.T, fixture *writerReclaimAuthorityFixture) {
				assertOneCapacityRowUpdated(t, fixture.connection, `
					UPDATE audit_events SET actor_role = 'administrator'
					WHERE tenant_id = ? AND sequence = ?`, writerTestTenant, fixture.auditSequence)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newWriterReclaimAuthorityFixture(t)
			defer fixture.close(t)
			test.tamper(t, fixture)
			stable := readWriterAuthoritySnapshot(t, fixture.harness.database)
			if _, err := fixture.currentWriter.Create(
				fixture.harness.actorCtx,
				fixture.harness.writeScope,
				capacityCreateRequest("reclaim-authority-rejected", "reclaim-authority-rejected-0001"),
			); !errors.Is(err, knowledgecatalog.ErrCorrupt) {
				t.Fatalf("Create() with corrupt reclaim %s error = %v, want ErrCorrupt", test.name, err)
			}
			assertWriterAuthoritySnapshotsEqual(
				t,
				readWriterAuthoritySnapshot(t, fixture.harness.database),
				stable,
			)
			assertCapacityReceiptExists(t, fixture.harness.database, fixture.corruptRequestID)
			test.restore(t, fixture)
			assertWriterCatalogIntegrity(t, fixture.harness.database)
		})
	}
}

type writerReclaimAuthorityFixture struct {
	harness          *writerBlackboxHarness
	currentWriter    *knowledgecatalog.Writer
	connection       *sql.Conn
	corruptRequestID string
	objectID         string
	objectVersion    int64
	createdAt        int64
	auditSequence    int64
	originalOutcome  []byte
}

func newWriterReclaimAuthorityFixture(t *testing.T) *writerReclaimAuthorityFixture {
	t.Helper()
	harness := newWriterBlackboxHarness(t)
	wallNow := time.Now().UTC().Truncate(time.Microsecond)
	oldWriter, _ := newCapacityTestWriter(
		t, harness, wallNow.Add(-366*24*time.Hour), 365*24*time.Hour, "ko_reclaim_authority_old",
	)
	oldRequest := capacityCreateRequest("reclaim-authority-old", "reclaim-authority-old-0001")
	if _, err := oldWriter.Create(harness.actorCtx, harness.writeScope, oldRequest); err != nil {
		t.Fatalf("create reclaim-authority old anchor: %v", err)
	}
	const corruptRequestID = "000-reclaim-authority-corrupt-00000000"
	seedCapacityReceiptCopies(t, harness.database, capacityReceiptSeed{
		SourceRequestID: oldRequest.GetClientRequestId(),
		RequestIDPrefix: "000-reclaim-authority-corrupt-",
		Count:           1,
		Retention:       7 * 24 * time.Hour,
	})
	currentWriter, _ := newCapacityTestWriter(
		t, harness, wallNow, 365*24*time.Hour, "ko_reclaim_authority_current",
	)
	currentRequest := capacityCreateRequest("reclaim-authority-current", "reclaim-authority-current-0001")
	if _, err := currentWriter.Create(harness.actorCtx, harness.writeScope, currentRequest); err != nil {
		t.Fatalf("create reclaim-authority current anchor: %v", err)
	}
	seedCapacityReceiptCopies(t, harness.database, capacityReceiptSeed{
		SourceRequestID: currentRequest.GetClientRequestId(),
		RequestIDPrefix: "reclaim-authority-current-filler-",
		Count:           writerNormalIdempotencyCapacity - 3,
		Retention:       365 * 24 * time.Hour,
	})
	connection, err := harness.database.SQLDB().Conn(t.Context())
	if err != nil {
		t.Fatalf("open reclaim-authority corruption connection: %v", err)
	}
	fixture := &writerReclaimAuthorityFixture{
		harness: harness, currentWriter: currentWriter, connection: connection,
		corruptRequestID: corruptRequestID,
	}
	if err := connection.QueryRowContext(t.Context(), `
		SELECT knowledge_object_id, object_version, created_at_unix_micro,
		       successful_audit_sequence
		FROM knowledge_mutation_idempotency
		WHERE tenant_id = ? AND client_request_id = ?`,
		writerTestTenant, corruptRequestID,
	).Scan(&fixture.objectID, &fixture.objectVersion, &fixture.createdAt, &fixture.auditSequence); err != nil {
		_ = connection.Close()
		t.Fatalf("read reclaim-authority receipt: %v", err)
	}
	return fixture
}

func (fixture *writerReclaimAuthorityFixture) dropTrigger(t *testing.T, name string) {
	t.Helper()
	if _, err := fixture.connection.ExecContext(t.Context(), "DROP TRIGGER "+name); err != nil {
		t.Fatalf("drop %s: %v", name, err)
	}
}

func (fixture *writerReclaimAuthorityFixture) updateReceipt(t *testing.T, assignment string, value any) {
	t.Helper()
	assertOneCapacityRowUpdated(t, fixture.connection, `
		UPDATE knowledge_mutation_idempotency SET `+assignment+`
		WHERE tenant_id = ? AND client_request_id = ?`,
		value, writerTestTenant, fixture.corruptRequestID,
	)
}

func (fixture *writerReclaimAuthorityFixture) setIgnoreChecks(t *testing.T, enabled bool) {
	t.Helper()
	value := "OFF"
	if enabled {
		value = "ON"
	}
	if _, err := fixture.connection.ExecContext(
		t.Context(), "PRAGMA ignore_check_constraints = "+value,
	); err != nil {
		t.Fatalf("set reclaim-authority check bypass %s: %v", value, err)
	}
}

func (fixture *writerReclaimAuthorityFixture) close(t *testing.T) {
	t.Helper()
	if err := fixture.connection.Close(); err != nil {
		t.Fatalf("close reclaim-authority corruption connection: %v", err)
	}
}
