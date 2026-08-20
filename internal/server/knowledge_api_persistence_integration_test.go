package server

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"path/filepath"
	"reflect"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/audit"
	"github.com/Suhaibinator/open-splunk/internal/auth"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/knowledgeattemptaudit"
	"github.com/Suhaibinator/open-splunk/internal/knowledgecatalog"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"gorm.io/gorm"
)

var knowledgePersistenceCursorKey = bytes.Repeat([]byte{0x6b}, 32)

type knowledgePersistenceHarness struct {
	database *control.DB
	catalog  *knowledgecatalog.Store
	writer   *knowledgecatalog.Writer
	audit    *audit.Store
	attempts *knowledgeattemptaudit.Store
	apps     *knowledgeHTTPAppCatalog
	handler  *apiHandler
	http     http.Handler
}

func newKnowledgePersistenceHarness(
	t *testing.T,
	writerAudit audit.TransactionAppender,
) *knowledgePersistenceHarness {
	t.Helper()

	database, err := control.Open(
		t.Context(),
		filepath.Join(t.TempDir(), "knowledge-http.sqlite"),
	)
	if err != nil {
		t.Fatalf("control.Open: %v", err)
	}
	t.Cleanup(func() {
		if err := database.Close(); err != nil {
			t.Errorf("close control database: %v", err)
		}
	})

	appCatalog, err := control.NewAppCatalog(database, control.AppCatalogOptions{
		CursorKey: knowledgePersistenceCursorKey,
		Clock: func() time.Time {
			return time.Date(2026, time.August, 7, 16, 0, 0, 0, time.UTC)
		},
		IDGenerator: func() (string, error) { return knowledgeHTTPAppID, nil },
	})
	if err != nil {
		t.Fatalf("control.NewAppCatalog: %v", err)
	}
	earliest := "-24h"
	latest := "now"
	if _, err := appCatalog.CreateApp(
		t.Context(),
		control.AppAccessScope{TenantID: knowledgeBoundaryTenantID},
		control.AppDefinition{
			Slug:        "knowledge-http-persistence",
			DisplayName: "Knowledge HTTP Persistence",
			DefaultTimeRange: &control.AppTimeRange{
				Earliest: &earliest,
				Latest:   &latest,
			},
		},
	); err != nil {
		t.Fatalf("CreateApp: %v", err)
	}

	auditStore, err := audit.NewStore(database, audit.StoreOptions{
		CursorKey: knowledgePersistenceCursorKey,
	})
	if err != nil {
		t.Fatalf("audit.NewStore: %v", err)
	}
	if writerAudit == nil {
		writerAudit = auditStore
	}
	catalog, err := knowledgecatalog.New(database, knowledgecatalog.Options{
		CursorKey: knowledgePersistenceCursorKey,
	})
	if err != nil {
		t.Fatalf("knowledgecatalog.New: %v", err)
	}
	attempts, err := knowledgeattemptaudit.NewWithContext(t.Context(), database)
	if err != nil {
		t.Fatalf("knowledgeattemptaudit.NewWithContext: %v", err)
	}

	var clockCalls atomic.Int64
	var idCalls atomic.Int64
	writer, err := knowledgecatalog.NewWriter(
		database,
		writerAudit,
		knowledgecatalog.WriterOptions{
			Clock: func() time.Time {
				return time.Date(2026, time.August, 7, 17, 0, 0, 0, time.UTC).
					Add(time.Duration(clockCalls.Add(1)) * time.Microsecond)
			},
			IDGenerator: func() (string, error) {
				return fmt.Sprintf("ko-persistence-%08d", idCalls.Add(1)), nil
			},
			IdempotencyRetention: 8 * 24 * time.Hour,
		},
	)
	if err != nil {
		t.Fatalf("knowledgecatalog.NewWriter: %v", err)
	}
	apps := knowledgeHTTPApps()
	api, httpHandler := newKnowledgeHTTPHandler(
		t,
		auth.BrowserRoleAdministrator,
		catalog,
		writer,
		apps,
		attempts,
	)
	return &knowledgePersistenceHarness{
		database: database,
		catalog:  catalog,
		writer:   writer,
		audit:    auditStore,
		attempts: attempts,
		apps:     apps,
		handler:  api,
		http:     httpHandler,
	}
}

func knowledgePersistenceOK(
	t *testing.T,
	handler http.Handler,
	path string,
	request proto.Message,
	response proto.Message,
) []byte {
	t.Helper()
	recorded := knowledgeHTTPPost(t, handler, path, request)
	if recorded.Code != http.StatusOK {
		t.Fatalf("POST %s status=%d body=%q", path, recorded.Code, recorded.Body.String())
	}
	if got := recorded.Header().Get("Content-Type"); got != "application/x-protobuf" {
		t.Fatalf("POST %s content type=%q", path, got)
	}
	if err := proto.Unmarshal(recorded.Body.Bytes(), response); err != nil {
		t.Fatalf("decode POST %s response: %v", path, err)
	}
	return bytes.Clone(recorded.Body.Bytes())
}

func knowledgePersistenceCount(
	t *testing.T,
	database *control.DB,
	query string,
	arguments ...any,
) int64 {
	t.Helper()
	var count int64
	if err := database.SQLDB().QueryRowContext(
		t.Context(), query, arguments...,
	).Scan(&count); err != nil {
		t.Fatalf("count persistence authority: %v", err)
	}
	return count
}

type knowledgePersistenceCatalogAuthority struct {
	catalogRevision     int64
	identityCount       int64
	versionCount        int64
	definitionBodyBytes int64
	idempotencyCount    int64
	activeObjectCount   int64
	recoveryAuditCount  int64
	headCatalogRevision int64
	stateToken          [32]byte
	projectionBytes     int64
}

func readKnowledgePersistenceCatalogAuthority(
	t *testing.T,
	database *control.DB,
) knowledgePersistenceCatalogAuthority {
	t.Helper()

	var authority knowledgePersistenceCatalogAuthority
	var stateToken []byte
	if err := database.SQLDB().QueryRowContext(t.Context(), `
		SELECT tenant.catalog_revision, tenant.identity_count,
		       tenant.version_count, tenant.definition_body_bytes,
		       tenant.idempotency_count, tenant.active_object_count,
		       tenant.recovery_audit_count, head.catalog_revision,
		       head.state_token, projection.projection_bytes
		FROM knowledge_catalog_tenants AS tenant
		JOIN knowledge_catalog_revision_heads AS head
		  ON head.tenant_id = tenant.tenant_id
		JOIN knowledge_projection_tenant_ledgers AS projection
		  ON projection.tenant_id = tenant.tenant_id
		WHERE tenant.tenant_id = ?`,
		knowledgeBoundaryTenantID,
	).Scan(
		&authority.catalogRevision,
		&authority.identityCount,
		&authority.versionCount,
		&authority.definitionBodyBytes,
		&authority.idempotencyCount,
		&authority.activeObjectCount,
		&authority.recoveryAuditCount,
		&authority.headCatalogRevision,
		&stateToken,
		&authority.projectionBytes,
	); err != nil {
		t.Fatalf("read provisioned catalog authority: %v", err)
	}
	if len(stateToken) != len(authority.stateToken) {
		t.Fatalf("provisioned catalog state token length=%d", len(stateToken))
	}
	copy(authority.stateToken[:], stateToken)
	return authority
}

func TestKnowledgeHTTPRealPersistenceLifecycleAndExactReplay(t *testing.T) {
	harness := newKnowledgePersistenceHarness(t, nil)

	createRequest := &opensplunk.CreateKnowledgeObjectRequest{
		Definition:      knowledgeHTTPDefinition(opensplunk.SharingScope_SHARING_SCOPE_PRIVATE),
		InitialState:    opensplunk.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DRAFT,
		ClientRequestId: "persistence-create-0001",
	}
	createResponse := &opensplunk.CreateKnowledgeObjectResponse{}
	createBytes := knowledgePersistenceOK(
		t, harness.http, knowledgeObjectsCreatePath, createRequest, createResponse,
	)
	objectID := createResponse.GetKnowledgeObject().GetKnowledgeObjectId()
	if objectID == "" || createResponse.GetKnowledgeObject().GetVersion() != 1 ||
		createResponse.GetKnowledgeObject().GetState() != opensplunk.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DRAFT ||
		createResponse.GetTenantCatalogRevision() != 1 ||
		len(createResponse.GetTenantCatalogStateToken()) != 32 {
		t.Fatalf("create response=%v", createResponse)
	}

	getResponse := &opensplunk.GetKnowledgeObjectResponse{}
	knowledgePersistenceOK(
		t,
		harness.http,
		knowledgeObjectsGetPath,
		&opensplunk.GetKnowledgeObjectRequest{KnowledgeObjectId: objectID},
		getResponse,
	)
	if !proto.Equal(getResponse.GetKnowledgeObject(), createResponse.GetKnowledgeObject()) {
		t.Fatalf("Get object=%v, want create object=%v", getResponse.GetKnowledgeObject(), createResponse.GetKnowledgeObject())
	}

	listResponse := &opensplunk.ListKnowledgeObjectsResponse{}
	pageSize := uint32(10)
	knowledgePersistenceOK(
		t,
		harness.http,
		knowledgeObjectsListPath,
		&opensplunk.ListKnowledgeObjectsRequest{Page: &opensplunk.PageRequest{
			PageSize:         &pageSize,
			IncludeTotalSize: true,
		}},
		listResponse,
	)
	if len(listResponse.GetKnowledgeObjects()) != 1 ||
		!proto.Equal(listResponse.GetKnowledgeObjects()[0], createResponse.GetKnowledgeObject()) ||
		listResponse.GetPage().GetTotalSize() != 1 ||
		!listResponse.GetPage().GetTotalSizeExact() ||
		listResponse.GetTenantCatalogRevision() != 1 {
		t.Fatalf("List response=%v", listResponse)
	}

	updatedDefinition := proto.Clone(createRequest.GetDefinition()).(*opensplunk.KnowledgeObjectDefinition)
	updatedDescription := "updated through the real HTTP persistence boundary"
	updatedDefinition.Description = &updatedDescription
	updateRequest := &opensplunk.UpdateKnowledgeObjectRequest{
		KnowledgeObjectId: objectID,
		ExpectedVersion:   1,
		Definition:        updatedDefinition,
		UpdateMask:        &fieldmaskpb.FieldMask{Paths: []string{"description"}},
		ClientRequestId:   "persistence-update-0001",
	}
	updateResponse := &opensplunk.UpdateKnowledgeObjectResponse{}
	updateBytes := knowledgePersistenceOK(
		t, harness.http, knowledgeObjectsUpdatePath, updateRequest, updateResponse,
	)
	if updateResponse.GetKnowledgeObject().GetVersion() != 2 ||
		updateResponse.GetKnowledgeObject().GetDefinition().GetDescription() != updatedDescription ||
		updateResponse.GetTenantCatalogRevision() != 2 {
		t.Fatalf("update response=%v", updateResponse)
	}

	disableRequest := &opensplunk.SetKnowledgeObjectStateRequest{
		KnowledgeObjectId: objectID,
		ExpectedVersion:   2,
		State:             opensplunk.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DISABLED,
		ClientRequestId:   "persistence-disable-0001",
	}
	disableResponse := &opensplunk.SetKnowledgeObjectStateResponse{}
	disableBytes := knowledgePersistenceOK(
		t, harness.http, knowledgeObjectsSetStatePath, disableRequest, disableResponse,
	)
	if disableResponse.GetKnowledgeObject().GetVersion() != 3 ||
		disableResponse.GetKnowledgeObject().GetState() != opensplunk.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DISABLED ||
		disableResponse.GetTenantCatalogRevision() != 3 {
		t.Fatalf("disable response=%v", disableResponse)
	}

	deleteRequest := &opensplunk.DeleteKnowledgeObjectRequest{
		KnowledgeObjectId: objectID,
		ExpectedVersion:   3,
		ClientRequestId:   "persistence-delete-0001",
	}
	deleteResponse := &opensplunk.DeleteKnowledgeObjectResponse{}
	deleteBytes := knowledgePersistenceOK(
		t, harness.http, knowledgeObjectsDeletePath, deleteRequest, deleteResponse,
	)
	if deleteResponse.GetKnowledgeObjectId() != objectID ||
		deleteResponse.GetDeletedVersion() != 4 ||
		deleteResponse.GetTenantCatalogRevision() != 4 {
		t.Fatalf("delete response=%v", deleteResponse)
	}

	replays := []struct {
		path     string
		request  proto.Message
		response proto.Message
		want     []byte
	}{
		{knowledgeObjectsCreatePath, createRequest, &opensplunk.CreateKnowledgeObjectResponse{}, createBytes},
		{knowledgeObjectsUpdatePath, updateRequest, &opensplunk.UpdateKnowledgeObjectResponse{}, updateBytes},
		{knowledgeObjectsSetStatePath, disableRequest, &opensplunk.SetKnowledgeObjectStateResponse{}, disableBytes},
		{knowledgeObjectsDeletePath, deleteRequest, &opensplunk.DeleteKnowledgeObjectResponse{}, deleteBytes},
	}
	for _, replay := range replays {
		got := knowledgePersistenceOK(t, harness.http, replay.path, replay.request, replay.response)
		if !bytes.Equal(got, replay.want) {
			t.Fatalf("exact replay %s bytes=%x, want %x", replay.path, got, replay.want)
		}
	}

	var revision, identities, versions, receipts, active int64
	if err := harness.database.SQLDB().QueryRowContext(t.Context(), `
		SELECT catalog_revision, identity_count, version_count,
		       idempotency_count, active_object_count
		FROM knowledge_catalog_tenants WHERE tenant_id = ?`,
		knowledgeBoundaryTenantID,
	).Scan(&revision, &identities, &versions, &receipts, &active); err != nil {
		t.Fatalf("read catalog ledger: %v", err)
	}
	if [5]int64{revision, identities, versions, receipts, active} != [5]int64{4, 1, 4, 4, 0} {
		t.Fatalf("catalog ledger revision/identities/versions/receipts/active=%v", [5]int64{revision, identities, versions, receipts, active})
	}

	type receiptAuthority struct {
		route        string
		mutation     string
		revision     int64
		objectID     string
		version      int64
		auditSeq     sql.NullInt64
		recoverySeq  sql.NullInt64
		catalogToken []byte
	}
	receiptRows, err := harness.database.SQLDB().QueryContext(t.Context(), `
		SELECT route, mutation_kind, committed_catalog_revision,
		       knowledge_object_id, object_version,
		       successful_audit_sequence, recovery_audit_sequence,
		       committed_catalog_state_token
		FROM knowledge_mutation_idempotency
		WHERE tenant_id = ? AND actor_kind = 'browser' AND actor_id = ?
		ORDER BY committed_catalog_revision`,
		knowledgeBoundaryTenantID,
		knowledgeBoundaryOwnerID,
	)
	if err != nil {
		t.Fatalf("read mutation receipts: %v", err)
	}
	defer receiptRows.Close()
	var gotReceipts []receiptAuthority
	for receiptRows.Next() {
		var receipt receiptAuthority
		if err := receiptRows.Scan(
			&receipt.route,
			&receipt.mutation,
			&receipt.revision,
			&receipt.objectID,
			&receipt.version,
			&receipt.auditSeq,
			&receipt.recoverySeq,
			&receipt.catalogToken,
		); err != nil {
			t.Fatalf("scan mutation receipt: %v", err)
		}
		gotReceipts = append(gotReceipts, receipt)
	}
	if err := receiptRows.Err(); err != nil {
		t.Fatalf("iterate mutation receipts: %v", err)
	}
	wantRoutes := []string{"objects.create", "objects.update", "objects.set_state", "objects.delete"}
	wantMutations := []string{"create", "update", "disable", "delete"}
	wantTokens := [][]byte{
		createResponse.GetTenantCatalogStateToken(),
		updateResponse.GetTenantCatalogStateToken(),
		disableResponse.GetTenantCatalogStateToken(),
		deleteResponse.GetTenantCatalogStateToken(),
	}
	if len(gotReceipts) != 4 {
		t.Fatalf("mutation receipts=%+v", gotReceipts)
	}
	for index, receipt := range gotReceipts {
		want := int64(index + 1)
		if receipt.route != wantRoutes[index] || receipt.mutation != wantMutations[index] ||
			receipt.revision != want || receipt.objectID != objectID || receipt.version != want ||
			!receipt.auditSeq.Valid || receipt.auditSeq.Int64 != want || receipt.recoverySeq.Valid ||
			!bytes.Equal(receipt.catalogToken, wantTokens[index]) {
			t.Fatalf("receipt[%d]=%+v", index, receipt)
		}
	}

	type auditAuthority struct {
		sequence int64
		action   string
		version  int64
	}
	auditRows, err := harness.database.SQLDB().QueryContext(t.Context(), `
		SELECT sequence, action, target_version FROM audit_events
		WHERE tenant_id = ? AND target_kind = 'knowledge_object'
		ORDER BY sequence`, knowledgeBoundaryTenantID)
	if err != nil {
		t.Fatalf("read mutation audit: %v", err)
	}
	defer auditRows.Close()
	var gotAudit []auditAuthority
	for auditRows.Next() {
		var event auditAuthority
		if err := auditRows.Scan(&event.sequence, &event.action, &event.version); err != nil {
			t.Fatalf("scan mutation audit: %v", err)
		}
		gotAudit = append(gotAudit, event)
	}
	if err := auditRows.Err(); err != nil {
		t.Fatalf("iterate mutation audit: %v", err)
	}
	wantAudit := []auditAuthority{
		{1, string(audit.ActionKnowledgeObjectCreate), 1},
		{2, string(audit.ActionKnowledgeObjectUpdate), 2},
		{3, string(audit.ActionKnowledgeObjectDisable), 3},
		{4, string(audit.ActionKnowledgeObjectDelete), 4},
	}
	if !reflect.DeepEqual(gotAudit, wantAudit) {
		t.Fatalf("mutation audit=%+v, want %+v", gotAudit, wantAudit)
	}
	if count := knowledgePersistenceCount(t, harness.database, `
		SELECT count(*) FROM knowledge_attempt_audit_events WHERE tenant_id = ?`,
		knowledgeBoundaryTenantID,
	); count != 0 {
		t.Fatalf("successful lifecycle rejected-attempt rows=%d", count)
	}
}

func TestKnowledgeHTTPRealStaleVersionJournalsCurrentAuthorizedContext(t *testing.T) {
	harness := newKnowledgePersistenceHarness(t, nil)
	create := &opensplunk.CreateKnowledgeObjectResponse{}
	knowledgePersistenceOK(t, harness.http, knowledgeObjectsCreatePath, &opensplunk.CreateKnowledgeObjectRequest{
		Definition:      knowledgeHTTPDefinition(opensplunk.SharingScope_SHARING_SCOPE_PRIVATE),
		InitialState:    opensplunk.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DRAFT,
		ClientRequestId: "stale-context-create-0001",
	}, create)
	objectID := create.GetKnowledgeObject().GetKnowledgeObjectId()
	definition := proto.Clone(create.GetKnowledgeObject().GetDefinition()).(*opensplunk.KnowledgeObjectDefinition)
	description := "this contender must not publish"
	definition.Description = &description

	response := knowledgeHTTPPost(t, harness.http, knowledgeObjectsUpdatePath, &opensplunk.UpdateKnowledgeObjectRequest{
		KnowledgeObjectId: objectID,
		ExpectedVersion:   2,
		Definition:        definition,
		UpdateMask:        &fieldmaskpb.FieldMask{Paths: []string{"description"}},
		ClientRequestId:   "stale-context-update-0001",
	})
	if response.Code != http.StatusConflict || response.Body.String() != "{\"error\":{\"message\":\"knowledge request conflicts with current state\"}}\n" {
		t.Fatalf("stale update status=%d body=%q", response.Code, response.Body.String())
	}

	var sequence, version int64
	var action, reason, appID, persistedObjectID, objectType, sharingScope string
	if err := harness.database.SQLDB().QueryRowContext(t.Context(), `
		SELECT sequence, action, reason, app_id, knowledge_object_id,
		       object_type, object_version, sharing_scope
		FROM knowledge_attempt_audit_events WHERE tenant_id = ?`,
		knowledgeBoundaryTenantID,
	).Scan(
		&sequence,
		&action,
		&reason,
		&appID,
		&persistedObjectID,
		&objectType,
		&version,
		&sharingScope,
	); err != nil {
		t.Fatalf("read stale-version rejected attempt: %v", err)
	}
	if sequence != 1 || action != string(knowledgeattemptaudit.ActionUpdate) ||
		reason != string(knowledgeattemptaudit.ReasonVersionConflict) ||
		appID != knowledgeHTTPAppID || persistedObjectID != objectID ||
		objectType != string(knowledgeattemptaudit.ObjectTypeFieldAlias) ||
		version != 1 || sharingScope != string(knowledgeattemptaudit.SharingScopePrivate) {
		t.Fatalf("stale attempt seq=%d action=%q reason=%q app=%q object=%q type=%q version=%d scope=%q", sequence, action, reason, appID, persistedObjectID, objectType, version, sharingScope)
	}
	if count := knowledgePersistenceCount(t, harness.database, `
		SELECT count(*) FROM knowledge_object_versions WHERE tenant_id = ?`,
		knowledgeBoundaryTenantID,
	); count != 1 {
		t.Fatalf("stale update version rows=%d, want 1", count)
	}
}

type knowledgePersistenceRedactingWriter struct {
	KnowledgeWriter
}

func (writer knowledgePersistenceRedactingWriter) Create(
	context.Context,
	knowledgecatalog.WriteScope,
	*opensplunk.CreateKnowledgeObjectRequest,
) (*opensplunk.CreateKnowledgeObjectResponse, error) {
	err := fmt.Errorf(
		"redacted persistence detail must never cross HTTP: %w",
		knowledgecatalog.ErrIdempotentOutcomeRedacted,
	)
	return nil, knowledgecatalog.WithErrorDisposition(
		err,
		knowledgecatalog.ErrorDispositionDefinitiveRejection,
	)
}

func TestKnowledgeHTTPMissingHiddenAndRedactedAreIndistinguishable(t *testing.T) {
	harness := newKnowledgePersistenceHarness(t, nil)
	actorContext, err := audit.WithActor(t.Context(), audit.Actor{
		Kind: audit.ActorKindBrowser,
		ID:   "hidden-object-seeder",
		Role: audit.ActorRoleAdministrator,
	})
	if err != nil {
		t.Fatalf("audit.WithActor: %v", err)
	}
	hiddenDefinition := knowledgeHTTPDefinition(opensplunk.SharingScope_SHARING_SCOPE_PRIVATE)
	hiddenDefinition.Name = "hidden-persistence-secret-name"
	hidden, err := harness.writer.Create(
		actorContext,
		knowledgecatalog.WriteScope{
			TenantID:       knowledgeBoundaryTenantID,
			OwnerID:        "hidden-persistence-owner",
			WritableAppIDs: []string{knowledgeHTTPAppID},
		},
		&opensplunk.CreateKnowledgeObjectRequest{
			Definition:      hiddenDefinition,
			InitialState:    opensplunk.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DRAFT,
			ClientRequestId: "hidden-object-create-0001",
		},
	)
	if err != nil {
		t.Fatalf("seed hidden object: %v", err)
	}
	hiddenID := hidden.GetKnowledgeObject().GetKnowledgeObjectId()

	missing := knowledgeHTTPPost(t, harness.http, knowledgeObjectsGetPath, &opensplunk.GetKnowledgeObjectRequest{
		KnowledgeObjectId: "ko-missing-persistence-secret",
	})
	hiddenResponse := knowledgeHTTPPost(t, harness.http, knowledgeObjectsGetPath, &opensplunk.GetKnowledgeObjectRequest{
		KnowledgeObjectId: hiddenID,
	})

	_, redactedHTTP := newKnowledgeHTTPHandler(
		t,
		auth.BrowserRoleAdministrator,
		harness.catalog,
		knowledgePersistenceRedactingWriter{KnowledgeWriter: harness.writer},
		harness.apps,
		harness.attempts,
	)
	redacted := knowledgeHTTPPost(t, redactedHTTP, knowledgeObjectsCreatePath, &opensplunk.CreateKnowledgeObjectRequest{
		Definition:      knowledgeHTTPDefinition(opensplunk.SharingScope_SHARING_SCOPE_PRIVATE),
		InitialState:    opensplunk.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DRAFT,
		ClientRequestId: "redacted-replay-0001",
	})

	responses := []*struct {
		Code int
		Body *bytes.Buffer
	}{
		{Code: missing.Code, Body: missing.Body},
		{Code: hiddenResponse.Code, Body: hiddenResponse.Body},
		{Code: redacted.Code, Body: redacted.Body},
	}
	for index, response := range responses {
		if response.Code != http.StatusNotFound ||
			!bytes.Equal(response.Body.Bytes(), responses[0].Body.Bytes()) {
			t.Fatalf("response[%d] status=%d body=%q; baseline status=%d body=%q", index, response.Code, response.Body.String(), responses[0].Code, responses[0].Body.String())
		}
		for _, secret := range []string{hiddenID, hiddenDefinition.GetName(), "redacted persistence detail", "ko-missing-persistence-secret"} {
			if bytes.Contains(response.Body.Bytes(), []byte(secret)) {
				t.Fatalf("response[%d] leaked %q in %q", index, secret, response.Body.String())
			}
		}
	}

	rows, err := harness.database.SQLDB().QueryContext(t.Context(), `
		SELECT action, reason, app_id, knowledge_object_id
		FROM knowledge_attempt_audit_events
		WHERE tenant_id = ? ORDER BY sequence`, knowledgeBoundaryTenantID)
	if err != nil {
		t.Fatalf("read nondisclosure attempts: %v", err)
	}
	defer rows.Close()
	var actions []string
	for rows.Next() {
		var action, reason string
		var appID, objectID sql.NullString
		if err := rows.Scan(&action, &reason, &appID, &objectID); err != nil {
			t.Fatalf("scan nondisclosure attempt: %v", err)
		}
		if reason != string(knowledgeattemptaudit.ReasonNotFoundOrForbidden) || appID.Valid || objectID.Valid {
			t.Fatalf("nondisclosure attempt action=%q reason=%q app=%v object=%v", action, reason, appID, objectID)
		}
		actions = append(actions, action)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate nondisclosure attempts: %v", err)
	}
	if !slices.Equal(actions, []string{
		string(knowledgeattemptaudit.ActionGet),
		string(knowledgeattemptaudit.ActionGet),
		string(knowledgeattemptaudit.ActionCreate),
	}) {
		t.Fatalf("nondisclosure attempt actions=%v", actions)
	}
}

type knowledgePersistenceFailingAuditAppender struct {
	err error
}

func (appender knowledgePersistenceFailingAuditAppender) AppendInTransaction(
	context.Context,
	*gorm.DB,
	string,
	audit.SuccessfulEvent,
) (audit.Event, error) {
	return audit.Event{}, appender.err
}

func TestKnowledgeHTTPSuccessAuditFailureRollsBackAndJournalsRejection(t *testing.T) {
	const privateFailure = "successful audit persistence exploded with private detail"
	harness := newKnowledgePersistenceHarness(t, knowledgePersistenceFailingAuditAppender{
		err: errors.New(privateFailure),
	})
	beforeAuthority := readKnowledgePersistenceCatalogAuthority(t, harness.database)
	if beforeAuthority.catalogRevision != 0 ||
		beforeAuthority.identityCount != 0 ||
		beforeAuthority.versionCount != 0 ||
		beforeAuthority.definitionBodyBytes != 0 ||
		beforeAuthority.idempotencyCount != 0 ||
		beforeAuthority.activeObjectCount != 0 ||
		beforeAuthority.recoveryAuditCount != 0 ||
		beforeAuthority.headCatalogRevision != 0 ||
		beforeAuthority.projectionBytes != 0 {
		t.Fatalf("initial provisioned catalog authority=%+v", beforeAuthority)
	}
	response := knowledgeHTTPPost(t, harness.http, knowledgeObjectsCreatePath, &opensplunk.CreateKnowledgeObjectRequest{
		Definition:      knowledgeHTTPDefinition(opensplunk.SharingScope_SHARING_SCOPE_PRIVATE),
		InitialState:    opensplunk.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DRAFT,
		ClientRequestId: "audit-failure-create-0001",
	})
	if response.Code != http.StatusServiceUnavailable ||
		response.Body.String() != knowledgeManagementUnavailableBody ||
		bytes.Contains(response.Body.Bytes(), []byte(privateFailure)) {
		t.Fatalf("audit failure status=%d body=%q", response.Code, response.Body.String())
	}

	for _, table := range []string{
		"knowledge_definition_blobs",
		"knowledge_objects",
		"knowledge_object_versions",
		"knowledge_object_version_lifecycle",
		"knowledge_object_dependencies",
		"knowledge_object_dependency_seals",
		"knowledge_object_list_projections",
		"knowledge_object_list_selector_patterns",
		"knowledge_object_list_projection_seals",
		"knowledge_object_list_order_keys",
		"knowledge_mutation_commit_authorities",
		"knowledge_mutation_idempotency",
		"audit_events",
	} {
		query := "SELECT count(*) FROM " + table + " WHERE tenant_id = ?" // #nosec G202 -- fixed test table names.
		if count := knowledgePersistenceCount(t, harness.database, query, knowledgeBoundaryTenantID); count != 0 {
			t.Fatalf("rolled-back %s rows=%d", table, count)
		}
	}
	if afterAuthority := readKnowledgePersistenceCatalogAuthority(t, harness.database); afterAuthority != beforeAuthority {
		t.Fatalf("catalog authority changed across rollback: before=%+v after=%+v", beforeAuthority, afterAuthority)
	}

	var sequence int64
	var action, reason, appID string
	var objectID sql.NullString
	if err := harness.database.SQLDB().QueryRowContext(t.Context(), `
		SELECT sequence, action, reason, app_id, knowledge_object_id
		FROM knowledge_attempt_audit_events WHERE tenant_id = ?`,
		knowledgeBoundaryTenantID,
	).Scan(&sequence, &action, &reason, &appID, &objectID); err != nil {
		t.Fatalf("read audit-failure attempt: %v", err)
	}
	if sequence != 1 || action != string(knowledgeattemptaudit.ActionCreate) ||
		reason != string(knowledgeattemptaudit.ReasonServiceUnavailable) ||
		appID != knowledgeHTTPAppID || objectID.Valid {
		t.Fatalf("audit-failure attempt sequence=%d action=%q reason=%q app=%q object=%v", sequence, action, reason, appID, objectID)
	}
}

func TestKnowledgeHTTPRejectedAttemptJournalFailureReturnsFixedUnavailable(t *testing.T) {
	harness := newKnowledgePersistenceHarness(t, nil)
	failedAttempts := &knowledgeBoundaryAppender{
		err: errors.New("attempt journal private failure detail"),
	}
	_, httpHandler := newKnowledgeHTTPHandler(
		t,
		auth.BrowserRoleAdministrator,
		harness.catalog,
		harness.writer,
		harness.apps,
		failedAttempts,
	)
	response := knowledgeHTTPPost(
		t,
		httpHandler,
		knowledgeObjectsCreatePath,
		&opensplunk.CreateKnowledgeObjectRequest{ClientRequestId: "invalid-before-writer-0001"},
	)
	if response.Code != http.StatusServiceUnavailable ||
		response.Body.String() != knowledgeManagementUnavailableBody {
		t.Fatalf("attempt failure status=%d body=%q", response.Code, response.Body.String())
	}
	calls := failedAttempts.snapshot()
	if len(calls) != 1 ||
		calls[0].definition.Action != knowledgeattemptaudit.ActionCreate ||
		calls[0].definition.Reason != knowledgeattemptaudit.ReasonInvalidDefinition {
		t.Fatalf("failed attempt calls=%+v", calls)
	}
	if count := knowledgePersistenceCount(t, harness.database, `
		SELECT count(*) FROM knowledge_attempt_audit_events WHERE tenant_id = ?`,
		knowledgeBoundaryTenantID,
	); count != 0 {
		t.Fatalf("failed attempt journal persisted rows=%d", count)
	}
	if count := knowledgePersistenceCount(t, harness.database, `
		SELECT count(*) FROM knowledge_objects WHERE tenant_id = ?`,
		knowledgeBoundaryTenantID,
	); count != 0 {
		t.Fatalf("invalid request persisted knowledge objects=%d", count)
	}
}
