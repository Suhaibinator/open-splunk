package knowledgecatalog_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"path/filepath"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/audit"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/knowledgecatalog"
)

const (
	writerTestTenant = "tenant-writer-blackbox"
	writerTestOwner  = "owner-writer-blackbox"
	writerTestApp    = "app_000000000200000000001A"
	writerTestAppTwo = "app_000000000200000000002A"
)

var writerTestCursorKey = []byte("knowledge-writer-blackbox-cursor-key-at-least-32-bytes")

type writerBlackboxHarness struct {
	database    *control.DB
	reader      *knowledgecatalog.Store
	writer      *knowledgecatalog.Writer
	audit       *audit.Store
	writeScope  knowledgecatalog.WriteScope
	readScope   knowledgecatalog.ReadScope
	actorCtx    context.Context
	idCalls     atomic.Int64
	clockCalls  atomic.Int64
	lastClockUS atomic.Int64
}

func newWriterBlackboxHarness(t *testing.T) *writerBlackboxHarness {
	t.Helper()

	database, err := control.Open(t.Context(), filepath.Join(t.TempDir(), "control.sqlite"))
	if err != nil {
		t.Fatalf("control.Open(): %v", err)
	}
	t.Cleanup(func() {
		if err := database.Close(); err != nil {
			t.Errorf("close control database: %v", err)
		}
	})

	var appClock atomic.Int64
	var appID atomic.Int64
	appIDs := []string{writerTestApp, writerTestAppTwo}
	apps, err := control.NewAppCatalog(database, control.AppCatalogOptions{
		CursorKey: writerTestCursorKey,
		Clock: func() time.Time {
			return time.UnixMicro(1_000 + appClock.Add(1)).UTC()
		},
		IDGenerator: func() (string, error) {
			index := int(appID.Add(1)) - 1
			if index < 0 || index >= len(appIDs) {
				return "", errors.New("writer black-box app ID sequence exhausted")
			}
			return appIDs[index], nil
		},
	})
	if err != nil {
		t.Fatalf("control.NewAppCatalog(): %v", err)
	}
	for _, slug := range []string{"writer-blackbox-one", "writer-blackbox-two"} {
		if _, err := apps.CreateApp(t.Context(), control.AppAccessScope{TenantID: writerTestTenant}, control.AppDefinition{
			Slug:        slug,
			DisplayName: slug,
			DefaultTimeRange: &control.AppTimeRange{
				Earliest: new("-24h"),
				Latest:   new("now"),
			},
		}); err != nil {
			t.Fatalf("CreateApp(%q): %v", slug, err)
		}
	}

	auditStore, err := audit.NewStore(database, audit.StoreOptions{CursorKey: writerTestCursorKey})
	if err != nil {
		t.Fatalf("audit.NewStore(): %v", err)
	}
	reader, err := knowledgecatalog.New(database, knowledgecatalog.Options{CursorKey: writerTestCursorKey})
	if err != nil {
		t.Fatalf("knowledgecatalog.New(): %v", err)
	}
	harness := &writerBlackboxHarness{
		database: database,
		reader:   reader,
		audit:    auditStore,
		writeScope: knowledgecatalog.WriteScope{
			TenantID:       writerTestTenant,
			OwnerID:        writerTestOwner,
			WritableAppIDs: []string{writerTestApp, writerTestAppTwo},
		},
		readScope: knowledgecatalog.ReadScope{
			TenantID:       writerTestTenant,
			OwnerID:        writerTestOwner,
			ReadableAppIDs: []string{writerTestApp, writerTestAppTwo},
		},
	}
	actorCtx, err := audit.WithActor(t.Context(), audit.Actor{
		Kind: audit.ActorKindBrowser,
		ID:   "writer-blackbox-administrator",
		Role: audit.ActorRoleAdministrator,
	})
	if err != nil {
		t.Fatalf("audit.WithActor(): %v", err)
	}
	harness.actorCtx = actorCtx

	writer, err := knowledgecatalog.NewWriter(database, auditStore, knowledgecatalog.WriterOptions{
		Clock: func() time.Time {
			call := harness.clockCalls.Add(1)
			value := 10_000 + call
			harness.lastClockUS.Store(value)
			return time.UnixMicro(value).UTC()
		},
		IDGenerator: func() (string, error) {
			call := harness.idCalls.Add(1)
			return fmt.Sprintf("ko_%022d", call), nil
		},
		IdempotencyRetention: 8 * 24 * time.Hour,
	})
	if err != nil {
		t.Fatalf("knowledgecatalog.NewWriter(): %v", err)
	}
	harness.writer = writer
	return harness
}

func writerAliasDefinition(
	appID string,
	name string,
	description *string,
	scope opensplunk.SharingScope,
	host string,
	source string,
	destination string,
) *opensplunk.KnowledgeObjectDefinition {
	definition := &opensplunk.KnowledgeObjectDefinition{
		AppId:        appID,
		Name:         name,
		Description:  description,
		SharingScope: scope,
		Body: &opensplunk.KnowledgeObjectDefinition_FieldAlias{
			FieldAlias: &opensplunk.FieldAliasDefinition{
				SourceField:       source,
				DestinationField:  destination,
				OverwriteBehavior: opensplunk.KnowledgeOverwriteBehavior_KNOWLEDGE_OVERWRITE_BEHAVIOR_PRESERVE_EXISTING,
			},
		},
	}
	if host != "" {
		definition.Selector = &opensplunk.KnowledgeSelector{
			HostPatterns: []*opensplunk.KnowledgeSelectorPattern{{
				MatchKind: opensplunk.KnowledgeSelectorMatchKind_KNOWLEDGE_SELECTOR_MATCH_KIND_EXACT,
				Value:     host,
			}},
		}
	}
	return definition
}

func (harness *writerBlackboxHarness) createDraft(
	t *testing.T,
	name string,
	requestID string,
) (*opensplunk.CreateKnowledgeObjectRequest, *opensplunk.CreateKnowledgeObjectResponse) {
	t.Helper()
	description := "draft description for " + name
	request := &opensplunk.CreateKnowledgeObjectRequest{
		Definition: writerAliasDefinition(
			writerTestApp,
			name,
			&description,
			opensplunk.SharingScope_SHARING_SCOPE_PRIVATE,
			"host-"+name,
			"source_field",
			"destination_"+name,
		),
		InitialState:    opensplunk.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DRAFT,
		ClientRequestId: requestID,
	}
	submitted := proto.Clone(request).(*opensplunk.CreateKnowledgeObjectRequest)
	response, err := harness.writer.Create(harness.actorCtx, harness.writeScope, request)
	if err != nil {
		t.Fatalf("Writer.Create(%q): %v", name, err)
	}
	if !proto.Equal(request, submitted) {
		t.Fatalf("Writer.Create(%q) mutated its caller-owned request: got %v want %v", name, request, submitted)
	}
	return request, response
}

type writerAuthoritySnapshot struct {
	CatalogRevision   int64
	CatalogStateToken [32]byte
	IdentityCount     int64
	VersionCount      int64
	DefinitionBytes   int64
	IdempotencyCount  int64
	ActiveObjectCount int64
	ProjectionBytes   int64
	AuditNextSequence int64
	AuditEventCount   int64
	TableCounts       map[string]int64
}

var writerAuthorityTableCounts = []struct {
	name      string
	statement string
}{
	{name: "knowledge_definition_blobs", statement: `SELECT count(*) FROM knowledge_definition_blobs WHERE tenant_id = ?`},
	{name: "knowledge_objects", statement: `SELECT count(*) FROM knowledge_objects WHERE tenant_id = ?`},
	{name: "knowledge_object_versions", statement: `SELECT count(*) FROM knowledge_object_versions WHERE tenant_id = ?`},
	{name: "knowledge_object_version_lifecycle", statement: `SELECT count(*) FROM knowledge_object_version_lifecycle WHERE tenant_id = ?`},
	{name: "knowledge_object_dependencies", statement: `SELECT count(*) FROM knowledge_object_dependencies WHERE tenant_id = ?`},
	{name: "knowledge_object_dependency_seals", statement: `SELECT count(*) FROM knowledge_object_dependency_seals WHERE tenant_id = ?`},
	{name: "knowledge_object_list_projections", statement: `SELECT count(*) FROM knowledge_object_list_projections WHERE tenant_id = ?`},
	{name: "knowledge_object_list_selector_patterns", statement: `SELECT count(*) FROM knowledge_object_list_selector_patterns WHERE tenant_id = ?`},
	{name: "knowledge_object_list_projection_seals", statement: `SELECT count(*) FROM knowledge_object_list_projection_seals WHERE tenant_id = ?`},
	{name: "knowledge_object_list_order_keys", statement: `SELECT count(*) FROM knowledge_object_list_order_keys WHERE tenant_id = ?`},
	{name: "knowledge_mutation_commit_authorities", statement: `SELECT count(*) FROM knowledge_mutation_commit_authorities WHERE tenant_id = ?`},
	{name: "knowledge_mutation_idempotency", statement: `SELECT count(*) FROM knowledge_mutation_idempotency WHERE tenant_id = ?`},
	{name: "audit_events", statement: `SELECT count(*) FROM audit_events WHERE tenant_id = ?`},
}

func readWriterAuthoritySnapshot(t *testing.T, database *control.DB) writerAuthoritySnapshot {
	t.Helper()
	tx, err := database.SQLDB().BeginTx(t.Context(), &sql.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatalf("begin writer authority snapshot: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	var snapshot writerAuthoritySnapshot
	var token []byte
	if err := tx.QueryRowContext(t.Context(), `
		SELECT tenant.catalog_revision, head.state_token,
		       tenant.identity_count, tenant.version_count,
		       tenant.definition_body_bytes, tenant.idempotency_count,
		       tenant.active_object_count
		FROM knowledge_catalog_tenants AS tenant
		JOIN knowledge_catalog_revision_heads AS head
		  ON head.tenant_id = tenant.tenant_id
		 AND head.catalog_revision = tenant.catalog_revision
		WHERE tenant.tenant_id = ?`, writerTestTenant).Scan(
		&snapshot.CatalogRevision,
		&token,
		&snapshot.IdentityCount,
		&snapshot.VersionCount,
		&snapshot.DefinitionBytes,
		&snapshot.IdempotencyCount,
		&snapshot.ActiveObjectCount,
	); err != nil {
		t.Fatalf("read writer catalog authority: %v", err)
	}
	if len(token) != len(snapshot.CatalogStateToken) {
		t.Fatalf("catalog state token bytes = %d, want %d", len(token), len(snapshot.CatalogStateToken))
	}
	copy(snapshot.CatalogStateToken[:], token)
	if err := tx.QueryRowContext(t.Context(), `
		SELECT projection_bytes
		FROM knowledge_projection_tenant_ledgers
		WHERE tenant_id = ?`, writerTestTenant).Scan(&snapshot.ProjectionBytes); err != nil {
		t.Fatalf("read writer projection ledger: %v", err)
	}
	if err := tx.QueryRowContext(t.Context(), `
		SELECT next_sequence, event_count
		FROM audit_tenant_state
		WHERE tenant_id = ?`, writerTestTenant).Scan(
		&snapshot.AuditNextSequence,
		&snapshot.AuditEventCount,
	); err != nil {
		t.Fatalf("read writer audit ledger: %v", err)
	}

	snapshot.TableCounts = make(map[string]int64, len(writerAuthorityTableCounts))
	for _, table := range writerAuthorityTableCounts {
		var count int64
		if err := tx.QueryRowContext(t.Context(), table.statement, writerTestTenant).Scan(&count); err != nil {
			t.Fatalf("count %s: %v", table.name, err)
		}
		snapshot.TableCounts[table.name] = count
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit writer authority snapshot: %v", err)
	}
	return snapshot
}

func assertWriterAuthoritySnapshotsEqual(
	t *testing.T,
	got writerAuthoritySnapshot,
	want writerAuthoritySnapshot,
) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("writer authority snapshot changed:\n got: %#v\nwant: %#v", got, want)
	}
}

func assertWriterCatalogIntegrity(t *testing.T, database *control.DB) {
	t.Helper()
	var integrity string
	if err := database.SQLDB().QueryRowContext(t.Context(), `PRAGMA integrity_check`).Scan(&integrity); err != nil {
		t.Fatalf("PRAGMA integrity_check: %v", err)
	}
	if integrity != "ok" {
		t.Fatalf("PRAGMA integrity_check = %q, want ok", integrity)
	}
	rows, err := database.SQLDB().QueryContext(t.Context(), `PRAGMA foreign_key_check`)
	if err != nil {
		t.Fatalf("PRAGMA foreign_key_check: %v", err)
	}
	defer rows.Close()
	if rows.Next() {
		t.Fatal("PRAGMA foreign_key_check reported a violation")
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate PRAGMA foreign_key_check: %v", err)
	}
}

func readCompactWriterOutcome(
	t *testing.T,
	database *control.DB,
	route string,
	requestID string,
) (*opensplunk.KnowledgeObjectVersionReference, []byte) {
	t.Helper()
	var encoded []byte
	if err := database.SQLDB().QueryRowContext(t.Context(), `
		SELECT outcome_proto
		FROM knowledge_mutation_idempotency
		WHERE tenant_id = ? AND actor_kind = ? AND actor_id = ?
		  AND route = ? AND client_request_id = ?`,
		writerTestTenant,
		string(audit.ActorKindBrowser),
		"writer-blackbox-administrator",
		route,
		requestID,
	).Scan(&encoded); err != nil {
		t.Fatalf("read compact writer outcome: %v", err)
	}
	outcome := &opensplunk.KnowledgeMutationOutcomeRecord{}
	if err := (proto.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(encoded, outcome); err != nil {
		t.Fatalf("decode compact writer outcome: %v", err)
	}
	if outcome.GetObject() == nil || outcome.GetRoute() != route ||
		len(outcome.ProtoReflect().GetUnknown()) != 0 ||
		len(outcome.GetObject().ProtoReflect().GetUnknown()) != 0 {
		t.Fatalf("compact writer outcome authority = %v", outcome)
	}
	canonical, err := (proto.MarshalOptions{Deterministic: true}).Marshal(outcome)
	if err != nil {
		t.Fatalf("re-encode compact writer outcome: %v", err)
	}
	if !bytes.Equal(encoded, canonical) {
		t.Fatalf("compact writer outcome is not deterministic: %x != %x", encoded, canonical)
	}
	return proto.Clone(outcome.GetObject()).(*opensplunk.KnowledgeObjectVersionReference), encoded
}

type writerIdempotencyReceipt struct {
	ActorKind                  string
	ActorID                    string
	MutationKind               string
	RequestDigestFormatVersion int64
	RequestDigest              []byte
	OutcomeFormatVersion       int64
	OutcomeProto               []byte
	CommittedCatalogRevision   int64
	CommittedCatalogStateToken []byte
	KnowledgeObjectID          string
	ObjectVersion              int64
	SuccessfulAuditSequence    sql.NullInt64
	RecoveryAuditSequence      sql.NullInt64
}

func readWriterIdempotencyReceipt(
	t *testing.T,
	database *control.DB,
	actorKind audit.ActorKind,
	actorID string,
	requestID string,
) writerIdempotencyReceipt {
	t.Helper()
	var receipt writerIdempotencyReceipt
	if err := database.SQLDB().QueryRowContext(t.Context(), `
		SELECT actor_kind, actor_id, mutation_kind,
		       request_digest_format_version, request_digest,
		       outcome_format_version, outcome_proto,
		       committed_catalog_revision, committed_catalog_state_token,
		       knowledge_object_id, object_version,
		       successful_audit_sequence, recovery_audit_sequence
		FROM knowledge_mutation_idempotency
		WHERE tenant_id = ? AND actor_kind = ? AND actor_id = ?
		  AND route = ? AND client_request_id = ?`,
		writerTestTenant,
		string(actorKind),
		actorID,
		"objects.create",
		requestID,
	).Scan(
		&receipt.ActorKind,
		&receipt.ActorID,
		&receipt.MutationKind,
		&receipt.RequestDigestFormatVersion,
		&receipt.RequestDigest,
		&receipt.OutcomeFormatVersion,
		&receipt.OutcomeProto,
		&receipt.CommittedCatalogRevision,
		&receipt.CommittedCatalogStateToken,
		&receipt.KnowledgeObjectID,
		&receipt.ObjectVersion,
		&receipt.SuccessfulAuditSequence,
		&receipt.RecoveryAuditSequence,
	); err != nil {
		t.Fatalf("read writer idempotency receipt: %v", err)
	}
	return receipt
}

func writerExpectedRequestDigest(
	t *testing.T,
	route string,
	ownerID string,
	request proto.Message,
) [sha256.Size]byte {
	t.Helper()
	cloned := proto.Clone(request)
	switch typed := cloned.(type) {
	case *opensplunk.CreateKnowledgeObjectRequest:
		typed.ClientRequestId = ""
	case *opensplunk.UpdateKnowledgeObjectRequest:
		typed.ClientRequestId = ""
	case *opensplunk.SetKnowledgeObjectStateRequest:
		typed.ClientRequestId = ""
	case *opensplunk.DeleteKnowledgeObjectRequest:
		typed.ClientRequestId = ""
	default:
		t.Fatalf("unsupported writer request digest type %T", request)
	}
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(cloned)
	if err != nil {
		t.Fatalf("marshal expected writer request digest: %v", err)
	}
	hasher := sha256.New()
	_, _ = hasher.Write([]byte("open-splunk/knowledge-mutation-request/v1\x00"))
	writerDigestFrame(hasher, []byte(route))
	writerDigestFrame(hasher, []byte(ownerID))
	writerDigestFrame(hasher, encoded)
	var result [sha256.Size]byte
	copy(result[:], hasher.Sum(nil))
	return result
}

func writerDigestFrame(hasher hash.Hash, value []byte) {
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(value)))
	_, _ = hasher.Write(length[:])
	_, _ = hasher.Write(value)
}

func knowledgeAuditEvents(t *testing.T, harness *writerBlackboxHarness) []audit.Event {
	t.Helper()
	targetKind := audit.TargetKindKnowledgeObject
	page, err := harness.audit.List(harness.actorCtx, writerTestTenant, audit.ListRequest{
		PageSize:   200,
		TargetKind: &targetKind,
	})
	if err != nil {
		t.Fatalf("audit.List(knowledge objects): %v", err)
	}
	if page.NextPageToken != "" {
		t.Fatal("knowledge audit fixture unexpectedly exceeded one page")
	}
	return page.Events
}
