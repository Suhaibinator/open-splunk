package knowledgecatalog

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/audit"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

const (
	writerFaultTenant = "tenant-writer-faults"
	writerFaultOwner  = "owner-writer-faults"
	writerFaultApp    = "app_000000000300000000001A"
)

var (
	writerFaultCursorKey = []byte("knowledge-writer-fault-cursor-key-at-least-32-bytes")
	errWriterFault       = errors.New("injected knowledge writer boundary failure")
)

func TestWriterRollsBackAtEveryCreatePrecommitBoundary(t *testing.T) {
	boundaries := []writerHookBoundary{
		writerHookPrepared,
		writerHookIdempotencyChecked,
		writerHookCatalogLedgersReady,
		writerHookCapacityChecked,
		writerHookDefinitionBlobReady,
		writerHookVersionInserted,
		writerHookDependencyRowsInserted,
		writerHookDependencySealed,
		writerHookProjectionInserted,
		writerHookSelectorRowsInserted,
		writerHookProjectionSealed,
		writerHookRegistryPublished,
		writerHookSuccessAuditAppended,
		writerHookCatalogRevisionAdvanced,
		writerHookCommitAuthorityRecorded,
		writerHookIdempotencyOutcomeRecorded,
		writerHookBeforeCommit,
	}
	for _, boundary := range boundaries {
		t.Run(string(boundary), func(t *testing.T) {
			harness := newWriterFaultHarness(t)
			before := readWriterFaultSnapshot(t, harness.database)
			request := writerFaultCreateRequest(
				"fault-"+string(boundary),
				"fault-"+string(boundary)+"-request-0001",
			)
			var targetCalls int
			harness.writer.hook = func(_ context.Context, event writerHookEvent) error {
				if event.Boundary == boundary {
					targetCalls++
					return errWriterFault
				}
				return nil
			}

			response, err := harness.writer.Create(harness.actorContext, harness.scope, request)
			if response != nil || !errors.Is(err, errWriterFault) {
				t.Fatalf("Create fault at %s = (%v, %v), want nil/injected error", boundary, response, err)
			}
			wantDisposition := ErrorDispositionDefinitiveRejection
			if boundary == writerHookPrepared {
				wantDisposition = ErrorDispositionIndeterminate
			}
			requireCatalogDisposition(t, err, wantDisposition)
			authorized, found := AuthorizedContextFromError(err)
			wantAuthorized := boundary != writerHookPrepared &&
				boundary != writerHookIdempotencyChecked &&
				boundary != writerHookCatalogLedgersReady
			if found != wantAuthorized {
				t.Fatalf("Create fault at %s authorization found = %v, want %v (%#v)", boundary, found, wantAuthorized, authorized)
			}
			if found && (authorized.AppID != writerFaultApp || authorized.Object != nil) {
				t.Fatalf("Create fault at %s authorization = %#v, want app-only", boundary, authorized)
			}
			if targetCalls != 1 {
				t.Fatalf("Create fault hook calls at %s = %d, want 1", boundary, targetCalls)
			}
			assertWriterFaultSnapshotsEqual(t, readWriterFaultSnapshot(t, harness.database), before)
			assertWriterFaultIntegrity(t, harness.database)

			harness.writer.hook = nil
			retried, err := harness.writer.Create(harness.actorContext, harness.scope, request)
			if err != nil {
				t.Fatalf("Create retry after %s rollback: %v", boundary, err)
			}
			if retried.GetKnowledgeObject().GetVersion() != 1 ||
				retried.GetKnowledgeObject().GetState() != opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DRAFT ||
				retried.GetTenantCatalogRevision() != 1 || len(retried.GetTenantCatalogStateToken()) != 32 {
				t.Fatalf("Create retry after %s = %v", boundary, retried)
			}
			assertWriterFaultCommittedCounts(t, harness.database, 1, 1)
			assertWriterFaultIntegrity(t, harness.database)
		})
	}
}

func TestWriterExistingObjectRoutesRollbackAtBeforeCommit(t *testing.T) {
	for _, route := range []string{"update", "disable", "delete"} {
		t.Run(route, func(t *testing.T) {
			harness := newWriterFaultHarness(t)
			created, err := harness.writer.Create(
				harness.actorContext,
				harness.scope,
				writerFaultCreateRequest("route-"+route, "route-"+route+"-create-0001"),
			)
			if err != nil {
				t.Fatalf("create %s baseline: %v", route, err)
			}
			object := created.GetKnowledgeObject()
			before := readWriterFaultSnapshot(t, harness.database)
			var targetCalls int
			harness.writer.hook = func(_ context.Context, event writerHookEvent) error {
				if event.Boundary == writerHookBeforeCommit {
					targetCalls++
					return errWriterFault
				}
				return nil
			}

			invoke := writerFaultRouteInvocation(t, harness, route, object)
			version, err := invoke()
			if version != 0 || !errors.Is(err, errWriterFault) {
				t.Fatalf("%s fault = (version %d, %v), want zero/injected error", route, version, err)
			}
			requireCatalogDisposition(t, err, ErrorDispositionDefinitiveRejection)
			authorized, found := AuthorizedContextFromError(err)
			if !found || authorized.AppID != writerFaultApp || authorized.Object == nil ||
				authorized.Object.KnowledgeObjectID != object.GetKnowledgeObjectId() ||
				authorized.Object.Version != 1 {
				t.Fatalf("%s before-commit authorization = %#v, found %v", route, authorized, found)
			}
			if targetCalls != 1 {
				t.Fatalf("%s before-commit hook calls = %d, want 1", route, targetCalls)
			}
			assertWriterFaultSnapshotsEqual(t, readWriterFaultSnapshot(t, harness.database), before)
			assertWriterFaultIntegrity(t, harness.database)

			harness.writer.hook = nil
			version, err = invoke()
			if err != nil || version != 2 {
				t.Fatalf("%s retry after rollback = (version %d, %v), want 2/nil", route, version, err)
			}
			assertWriterFaultCommittedCounts(t, harness.database, 2, 2)
			assertWriterFaultIntegrity(t, harness.database)
		})
	}
}

func TestWriterAfterCommitLostResponseReplaysAfterReopen(t *testing.T) {
	harness := newWriterFaultHarness(t)
	request := writerFaultCreateRequest("lost-response", "lost-response-request-0001")
	var afterCommitCalls int
	harness.writer.hook = func(_ context.Context, event writerHookEvent) error {
		if event.Boundary == writerHookAfterCommit {
			afterCommitCalls++
			return errWriterFault
		}
		return nil
	}

	response, err := harness.writer.Create(harness.actorContext, harness.scope, request)
	if response != nil || !errors.Is(err, errWriterFault) {
		t.Fatalf("Create lost response = (%v, %v), want nil/injected error", response, err)
	}
	requireCatalogDisposition(t, err, ErrorDispositionKnownCommitted)
	if authorized, found := AuthorizedContextFromError(err); !found ||
		authorized.AppID != writerFaultApp || authorized.Object != nil {
		t.Fatalf("lost-response authorization = %#v, found %v; want app-only", authorized, found)
	}
	if afterCommitCalls != 1 {
		t.Fatalf("after-commit hook calls = %d, want 1", afterCommitCalls)
	}
	committed := readWriterFaultSnapshot(t, harness.database)
	assertWriterFaultCommittedCounts(t, harness.database, 1, 1)
	assertWriterFaultIntegrity(t, harness.database)

	databasePath := harness.path
	harness.close(t)
	reopened, err := control.Open(t.Context(), databasePath)
	if err != nil {
		t.Fatalf("reopen lost-response database: %v", err)
	}
	harness.database = reopened
	auditStore, err := audit.NewStore(reopened, audit.StoreOptions{CursorKey: writerFaultCursorKey})
	if err != nil {
		t.Fatalf("audit.NewStore(reopen): %v", err)
	}
	var replayIDCalls atomic.Int64
	reopenedWriter, err := NewWriter(reopened, auditStore, WriterOptions{
		Clock: func() time.Time { return time.UnixMicro(9_999_999).UTC() },
		IDGenerator: func() (string, error) {
			replayIDCalls.Add(1)
			return "", errors.New("replay unexpectedly requested a new object identity")
		},
		IdempotencyRetention: 8 * 24 * time.Hour,
	})
	if err != nil {
		t.Fatalf("NewWriter(reopen): %v", err)
	}

	replayed, err := reopenedWriter.Create(harness.actorContext, harness.scope, request)
	if err != nil {
		t.Fatalf("replay lost response after reopen: %v", err)
	}
	if replayIDCalls.Load() != 0 || replayed.GetKnowledgeObject() == nil ||
		replayed.GetKnowledgeObject().GetVersion() != 1 ||
		replayed.GetKnowledgeObject().GetName() != request.GetDefinition().GetName() ||
		replayed.GetTenantCatalogRevision() != 1 || len(replayed.GetTenantCatalogStateToken()) != 32 {
		t.Fatalf("lost-response replay = %v, ID calls = %d", replayed, replayIDCalls.Load())
	}
	assertWriterFaultSnapshotsEqual(t, readWriterFaultSnapshot(t, reopened), committed)
	assertWriterFaultIntegrity(t, reopened)
}

type writerFaultHarness struct {
	database     *control.DB
	path         string
	writer       *Writer
	actorContext context.Context
	scope        WriteScope
	idCalls      atomic.Int64
	clockCalls   atomic.Int64
}

func newWriterFaultHarness(t *testing.T) *writerFaultHarness {
	t.Helper()
	harness := &writerFaultHarness{path: filepath.Join(t.TempDir(), "control.sqlite")}
	database, err := control.Open(t.Context(), harness.path)
	if err != nil {
		t.Fatalf("control.Open(): %v", err)
	}
	harness.database = database
	t.Cleanup(func() {
		if harness.database != nil {
			if err := harness.database.Close(); err != nil {
				t.Errorf("close writer fault database: %v", err)
			}
		}
	})

	apps, err := control.NewAppCatalog(database, control.AppCatalogOptions{
		CursorKey: writerFaultCursorKey,
		Clock: func() time.Time {
			return time.UnixMicro(2_000).UTC()
		},
		IDGenerator: func() (string, error) { return writerFaultApp, nil },
	})
	if err != nil {
		t.Fatalf("control.NewAppCatalog(): %v", err)
	}
	if _, err := apps.CreateApp(t.Context(), control.AppAccessScope{TenantID: writerFaultTenant}, control.AppDefinition{
		Slug:        "writer-fault-app",
		DisplayName: "Writer fault app",
	}); err != nil {
		t.Fatalf("CreateApp(): %v", err)
	}
	auditStore, err := audit.NewStore(database, audit.StoreOptions{CursorKey: writerFaultCursorKey})
	if err != nil {
		t.Fatalf("audit.NewStore(): %v", err)
	}
	actorContext, err := audit.WithActor(t.Context(), audit.Actor{
		Kind: audit.ActorKindBrowser,
		ID:   "writer-fault-administrator",
		Role: audit.ActorRoleAdministrator,
	})
	if err != nil {
		t.Fatalf("audit.WithActor(): %v", err)
	}
	harness.actorContext = actorContext
	harness.scope = WriteScope{
		TenantID:       writerFaultTenant,
		OwnerID:        writerFaultOwner,
		WritableAppIDs: []string{writerFaultApp},
	}
	writer, err := NewWriter(database, auditStore, WriterOptions{
		Clock: func() time.Time {
			return time.UnixMicro(10_000 + harness.clockCalls.Add(1)).UTC()
		},
		IDGenerator: func() (string, error) {
			return fmt.Sprintf("ko_fault_%020d", harness.idCalls.Add(1)), nil
		},
		IdempotencyRetention: 8 * 24 * time.Hour,
	})
	if err != nil {
		t.Fatalf("NewWriter(): %v", err)
	}
	harness.writer = writer
	return harness
}

func (harness *writerFaultHarness) close(t *testing.T) {
	t.Helper()
	if harness.database == nil {
		return
	}
	if err := harness.database.Close(); err != nil {
		t.Fatalf("close writer fault database: %v", err)
	}
	harness.database = nil
}

func writerFaultCreateRequest(name, requestID string) *opensplunkv1.CreateKnowledgeObjectRequest {
	description := "fault-injection definition for " + name
	return &opensplunkv1.CreateKnowledgeObjectRequest{
		Definition: &opensplunkv1.KnowledgeObjectDefinition{
			AppId:        writerFaultApp,
			Name:         name,
			Description:  &description,
			SharingScope: opensplunkv1.SharingScope_SHARING_SCOPE_PRIVATE,
			Selector: &opensplunkv1.KnowledgeSelector{
				HostPatterns: []*opensplunkv1.KnowledgeSelectorPattern{{
					MatchKind: opensplunkv1.KnowledgeSelectorMatchKind_KNOWLEDGE_SELECTOR_MATCH_KIND_EXACT,
					Value:     "fault-host-" + name,
				}},
			},
			Body: &opensplunkv1.KnowledgeObjectDefinition_FieldAlias{
				FieldAlias: &opensplunkv1.FieldAliasDefinition{
					SourceField:       "source_field",
					DestinationField:  "destination_field",
					OverwriteBehavior: opensplunkv1.KnowledgeOverwriteBehavior_KNOWLEDGE_OVERWRITE_BEHAVIOR_PRESERVE_EXISTING,
				},
			},
		},
		InitialState:    opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DRAFT,
		ClientRequestId: requestID,
	}
}

func writerFaultRouteInvocation(
	t *testing.T,
	harness *writerFaultHarness,
	route string,
	object *opensplunkv1.KnowledgeObject,
) func() (uint64, error) {
	t.Helper()
	switch route {
	case "update":
		definition := proto.Clone(object.GetDefinition()).(*opensplunkv1.KnowledgeObjectDefinition)
		description := "updated through fault injection"
		definition.Description = &description
		request := &opensplunkv1.UpdateKnowledgeObjectRequest{
			KnowledgeObjectId: object.GetKnowledgeObjectId(),
			ExpectedVersion:   1,
			Definition:        definition,
			UpdateMask:        &fieldmaskpb.FieldMask{Paths: []string{"description"}},
			ClientRequestId:   "route-update-request-0001",
		}
		return func() (uint64, error) {
			response, err := harness.writer.Update(harness.actorContext, harness.scope, request)
			if response == nil {
				return 0, err
			}
			return response.GetKnowledgeObject().GetVersion(), err
		}
	case "disable":
		request := &opensplunkv1.SetKnowledgeObjectStateRequest{
			KnowledgeObjectId: object.GetKnowledgeObjectId(),
			ExpectedVersion:   1,
			State:             opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DISABLED,
			ClientRequestId:   "route-disable-request-001",
		}
		return func() (uint64, error) {
			response, err := harness.writer.SetState(harness.actorContext, harness.scope, request)
			if response == nil {
				return 0, err
			}
			return response.GetKnowledgeObject().GetVersion(), err
		}
	case "delete":
		request := &opensplunkv1.DeleteKnowledgeObjectRequest{
			KnowledgeObjectId: object.GetKnowledgeObjectId(),
			ExpectedVersion:   1,
			ClientRequestId:   "route-delete-request-0001",
		}
		return func() (uint64, error) {
			response, err := harness.writer.Delete(harness.actorContext, harness.scope, request)
			if response == nil {
				return 0, err
			}
			return response.GetDeletedVersion(), err
		}
	default:
		t.Fatalf("unsupported writer fault route %q", route)
		return nil
	}
}

var writerFaultSnapshotTables = []string{
	"audit_events",
	"audit_tenant_state",
	"knowledge_app_active_counters",
	"knowledge_app_type_active_counters",
	"knowledge_catalog_revision_heads",
	"knowledge_catalog_tenants",
	"knowledge_definition_blobs",
	"knowledge_mutation_commit_authorities",
	"knowledge_mutation_idempotency",
	"knowledge_object_acl",
	"knowledge_object_dependencies",
	"knowledge_object_dependency_seals",
	"knowledge_object_list_order_keys",
	"knowledge_object_list_projection_seals",
	"knowledge_object_list_projections",
	"knowledge_object_list_selector_patterns",
	"knowledge_object_version_lifecycle",
	"knowledge_object_versions",
	"knowledge_objects",
	"knowledge_owner_active_counters",
	"knowledge_projection_tenant_ledgers",
	"knowledge_recovery_audit",
	"knowledge_type_active_counters",
}

type writerFaultSnapshot map[string][]string

func readWriterFaultSnapshot(t *testing.T, database *control.DB) writerFaultSnapshot {
	t.Helper()
	snapshot := make(writerFaultSnapshot, len(writerFaultSnapshotTables))
	for _, table := range writerFaultSnapshotTables {
		query := `SELECT * FROM "` + table + `" WHERE tenant_id = ?`
		rows, err := database.SQLDB().QueryContext(t.Context(), query, writerFaultTenant)
		if err != nil {
			t.Fatalf("snapshot %s: %v", table, err)
		}
		columns, err := rows.Columns()
		if err != nil {
			_ = rows.Close()
			t.Fatalf("snapshot %s columns: %v", table, err)
		}
		encodedRows := make([]string, 0)
		for rows.Next() {
			values := make([]any, len(columns))
			destinations := make([]any, len(columns))
			for index := range values {
				destinations[index] = &values[index]
			}
			if err := rows.Scan(destinations...); err != nil {
				_ = rows.Close()
				t.Fatalf("snapshot %s row: %v", table, err)
			}
			var encoded strings.Builder
			for index, value := range values {
				encoded.WriteString(strconv.Quote(columns[index]))
				encoded.WriteByte('=')
				encoded.WriteString(writerFaultSQLValue(value))
				encoded.WriteByte(';')
			}
			encodedRows = append(encodedRows, encoded.String())
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			t.Fatalf("snapshot %s iteration: %v", table, err)
		}
		if err := rows.Close(); err != nil {
			t.Fatalf("snapshot %s close: %v", table, err)
		}
		sort.Strings(encodedRows)
		snapshot[table] = encodedRows
	}
	return snapshot
}

func writerFaultSQLValue(value any) string {
	switch typed := value.(type) {
	case nil:
		return "null"
	case int64:
		return "integer:" + strconv.FormatInt(typed, 10)
	case float64:
		return "float:" + strconv.FormatFloat(typed, 'g', -1, 64)
	case bool:
		return "boolean:" + strconv.FormatBool(typed)
	case string:
		return "text:" + strconv.Quote(typed)
	case []byte:
		return "blob:" + hex.EncodeToString(typed)
	case time.Time:
		return "time:" + typed.UTC().Format(time.RFC3339Nano)
	default:
		return fmt.Sprintf("%T:%v", value, value)
	}
}

func assertWriterFaultSnapshotsEqual(t *testing.T, got, want writerFaultSnapshot) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("writer authority changed across rollback:\n got: %#v\nwant: %#v", got, want)
	}
}

func assertWriterFaultCommittedCounts(t *testing.T, database *control.DB, versions, auditEvents int64) {
	t.Helper()
	want := map[string]int64{
		"knowledge_objects":                     1,
		"knowledge_object_versions":             versions,
		"knowledge_object_version_lifecycle":    versions,
		"knowledge_object_dependency_seals":     versions,
		"knowledge_mutation_commit_authorities": versions,
		"knowledge_mutation_idempotency":        versions,
		"audit_events":                          auditEvents,
	}
	for table, expected := range want {
		var count int64
		query := `SELECT count(*) FROM "` + table + `" WHERE tenant_id = ?`
		if err := database.SQLDB().QueryRowContext(t.Context(), query, writerFaultTenant).Scan(&count); err != nil {
			t.Fatalf("count %s: %v", table, err)
		}
		if count != expected {
			t.Errorf("%s rows = %d, want %d", table, count, expected)
		}
	}
}

func assertWriterFaultIntegrity(t *testing.T, database *control.DB) {
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
