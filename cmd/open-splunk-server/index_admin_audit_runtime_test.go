package main

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"path/filepath"
	"strings"
	"testing"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/audit"
	"github.com/Suhaibinator/open-splunk/internal/auth"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/server"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"gorm.io/gorm"
)

const (
	runtimeIndexAuditTenantID   = "tenant-a"
	runtimeIndexAuditOwnerID    = "administrator"
	runtimeIndexAuditEventCount = 9
)

func TestRuntimeIndexAdministrationPublishesAuthenticatedAudit(t *testing.T) {
	ctx := context.Background()
	directory := t.TempDir()
	databasePath := filepath.Join(directory, "control.db")
	masterKeyPath := filepath.Join(directory, "server.key")
	bearerToken := bytes.Repeat(
		[]byte("i"),
		auth.MinimumBrowserBearerTokenBytes,
	)
	authenticator := newRuntimeIndexAuditAuthenticator(
		t,
		bearerToken,
		runtimeIndexAuditTenantID,
	)

	firstDatabase, err := control.Open(ctx, databasePath)
	if err != nil {
		t.Fatal(err)
	}
	firstDatabaseClosed := false
	t.Cleanup(func() {
		if !firstDatabaseClosed {
			if err := firstDatabase.Close(); err != nil {
				t.Errorf("close first control database: %v", err)
			}
		}
	})
	firstStores, err := openRuntimeSecurityStores(
		ctx,
		firstDatabase,
		masterKeyPath,
		runtimeIndexAuditTenantID,
	)
	if err != nil {
		t.Fatal(err)
	}
	firstAdministration, err := newRuntimeIndexAdministration(
		firstDatabase,
		runtimeIndexAuditTenantID,
		firstStores.auditEvents,
	)
	if err != nil {
		t.Fatal(err)
	}
	firstHandler := newRuntimeIndexAuditHandlerForTest(
		t,
		firstDatabase,
		firstAdministration,
		firstStores.auditEvents,
		authenticator,
		&runtimeIndexAuditWaker{},
	)

	dataIndexName := "runtime-audit-delete-data"
	dataDisplayName := "Definition payload must not appear 4817"
	dataDescription := "create-request-payload-must-not-appear-6829"
	updatedDescription := "update-request-payload-must-not-appear-7341"
	keepIndexName := "runtime-audit-keep-data"
	keepDisplayName := "Keep definition payload must not appear 8413"
	keepDescription := "keep-request-payload-must-not-appear-9625"
	payloadCanaries := []string{
		dataIndexName,
		dataDisplayName,
		dataDescription,
		updatedDescription,
		keepIndexName,
		keepDisplayName,
		keepDescription,
	}

	dataIndex := createRuntimeIndexForAudit(
		t,
		firstHandler,
		bearerToken,
		dataIndexName,
		dataDisplayName,
		dataDescription,
	)
	dataIndex = updateRuntimeIndexDescriptionForAudit(
		t,
		firstHandler,
		bearerToken,
		dataIndex,
		updatedDescription,
	)
	dataIndex = setRuntimeIndexStateForAudit(
		t,
		firstHandler,
		bearerToken,
		dataIndex,
		opensplunk.IndexState_INDEX_STATE_ARCHIVED,
	)
	dataIndex = setRuntimeIndexStateForAudit(
		t,
		firstHandler,
		bearerToken,
		dataIndex,
		opensplunk.IndexState_INDEX_STATE_ACTIVE,
	)
	dataIndex = setRuntimeIndexStateForAudit(
		t,
		firstHandler,
		bearerToken,
		dataIndex,
		opensplunk.IndexState_INDEX_STATE_ARCHIVED,
	)
	if dataIndex.GetVersion() != 5 {
		t.Fatalf("data index version = %d, want 5", dataIndex.GetVersion())
	}
	deleteDataRequest := &opensplunk.DeleteIndexRequest{
		Selector:         runtimeIndexAuditSelector(dataIndex.GetIndexId()),
		ExpectedVersion:  dataIndex.GetVersion(),
		DataDeletionMode: opensplunk.IndexDataDeletionMode_INDEX_DATA_DELETION_MODE_DELETE_DATA,
		ConfirmationName: dataIndexName,
	}
	firstDeletion := deleteRuntimeIndexForAudit(
		t,
		firstHandler,
		bearerToken,
		deleteDataRequest,
	)
	retriedDeletion := deleteRuntimeIndexForAudit(
		t,
		firstHandler,
		bearerToken,
		deleteDataRequest,
	)
	if firstDeletion.GetIndexId() != dataIndex.GetIndexId() ||
		firstDeletion.GetDeletionOperationId() == "" ||
		retriedDeletion.GetDeletionOperationId() !=
			firstDeletion.GetDeletionOperationId() {
		t.Fatalf(
			"DELETE_DATA retry = (%q, %q), first = (%q, %q)",
			retriedDeletion.GetIndexId(),
			retriedDeletion.GetDeletionOperationId(),
			firstDeletion.GetIndexId(),
			firstDeletion.GetDeletionOperationId(),
		)
	}

	keepIndex := createRuntimeIndexForAudit(
		t,
		firstHandler,
		bearerToken,
		keepIndexName,
		keepDisplayName,
		keepDescription,
	)
	keepIndex = setRuntimeIndexStateForAudit(
		t,
		firstHandler,
		bearerToken,
		keepIndex,
		opensplunk.IndexState_INDEX_STATE_ARCHIVED,
	)
	keepDeletion := deleteRuntimeIndexForAudit(
		t,
		firstHandler,
		bearerToken,
		&opensplunk.DeleteIndexRequest{
			Selector:         runtimeIndexAuditSelector(keepIndex.GetIndexId()),
			ExpectedVersion:  keepIndex.GetVersion(),
			DataDeletionMode: opensplunk.IndexDataDeletionMode_INDEX_DATA_DELETION_MODE_KEEP_DATA,
			ConfirmationName: keepIndexName,
		},
	)
	if keepDeletion.GetIndexId() != keepIndex.GetIndexId() ||
		keepDeletion.GetDeletionOperationId() != "" {
		t.Fatalf("KEEP_DATA deletion = %+v", keepDeletion)
	}

	firstPageSize := uint32(5)
	indexTargetKind := opensplunk.AuditTargetKind_AUDIT_TARGET_KIND_INDEX
	var firstPage opensplunk.ListAuditEventsResponse
	firstPagePayload := postRuntimeProtoOK(
		t,
		firstHandler,
		"/api/audit/events/list",
		&opensplunk.ListAuditEventsRequest{
			Page: &opensplunk.PageRequest{
				PageSize:         &firstPageSize,
				IncludeTotalSize: true,
			},
			TargetKindFilter: &indexTargetKind,
		},
		&firstPage,
		bearerToken,
	)
	firstPageMetadata := firstPage.GetPage()
	if len(firstPage.GetAuditEvents()) != int(firstPageSize) ||
		firstPageMetadata == nil ||
		firstPageMetadata.TotalSize == nil ||
		firstPageMetadata.GetTotalSize() != runtimeIndexAuditEventCount ||
		!firstPageMetadata.GetTotalSizeExact() ||
		firstPageMetadata.GetNextPageToken() == "" {
		t.Fatalf("first audit page = %+v", &firstPage)
	}

	if err := firstHandler.Close(ctx); err != nil {
		t.Fatalf("close first handler: %v", err)
	}
	if err := firstDatabase.Close(); err != nil {
		t.Fatalf("close first control database: %v", err)
	}
	firstDatabaseClosed = true

	secondDatabase, err := control.Open(ctx, databasePath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := secondDatabase.Close(); err != nil {
			t.Errorf("close second control database: %v", err)
		}
	})
	secondStores, err := openRuntimeSecurityStores(
		ctx,
		secondDatabase,
		masterKeyPath,
		runtimeIndexAuditTenantID,
	)
	if err != nil {
		t.Fatal(err)
	}
	secondAdministration, err := newRuntimeIndexAdministration(
		secondDatabase,
		runtimeIndexAuditTenantID,
		secondStores.auditEvents,
	)
	if err != nil {
		t.Fatal(err)
	}
	secondHandler := newRuntimeIndexAuditHandlerForTest(
		t,
		secondDatabase,
		secondAdministration,
		secondStores.auditEvents,
		authenticator,
		&runtimeIndexAuditWaker{},
	)
	reopenedRetry := deleteRuntimeIndexForAudit(
		t,
		secondHandler,
		bearerToken,
		deleteDataRequest,
	)
	if reopenedRetry.GetIndexId() != dataIndex.GetIndexId() ||
		reopenedRetry.GetDeletionOperationId() !=
			firstDeletion.GetDeletionOperationId() {
		t.Fatalf(
			"DELETE_DATA retry after reopen = (%q, %q), want (%q, %q)",
			reopenedRetry.GetIndexId(),
			reopenedRetry.GetDeletionOperationId(),
			firstDeletion.GetIndexId(),
			firstDeletion.GetDeletionOperationId(),
		)
	}

	secondPageSize := firstPageSize
	continuation := firstPageMetadata.GetNextPageToken()
	var secondPage opensplunk.ListAuditEventsResponse
	secondPagePayload := postRuntimeProtoOK(
		t,
		secondHandler,
		"/api/audit/events/list",
		&opensplunk.ListAuditEventsRequest{
			Page: &opensplunk.PageRequest{
				PageSize:         &secondPageSize,
				PageToken:        &continuation,
				IncludeTotalSize: true,
			},
			TargetKindFilter: &indexTargetKind,
		},
		&secondPage,
		bearerToken,
	)
	secondPageMetadata := secondPage.GetPage()
	if len(secondPage.GetAuditEvents()) !=
		runtimeIndexAuditEventCount-int(secondPageSize) ||
		secondPageMetadata == nil ||
		secondPageMetadata.TotalSize == nil ||
		secondPageMetadata.GetTotalSize() != runtimeIndexAuditEventCount ||
		!secondPageMetadata.GetTotalSizeExact() ||
		secondPageMetadata.GetNextPageToken() != "" {
		t.Fatalf("second audit page after reopen = %+v", &secondPage)
	}

	listedEvents := append(
		append(
			[]*opensplunk.AuditEvent(nil),
			firstPage.GetAuditEvents()...,
		),
		secondPage.GetAuditEvents()...,
	)
	expectations := []runtimeIndexAuditExpectation{
		{opensplunk.AuditAction_AUDIT_ACTION_INDEX_DELETE_KEEP_DATA, keepIndex.GetIndexId(), 2},
		{opensplunk.AuditAction_AUDIT_ACTION_INDEX_ARCHIVE, keepIndex.GetIndexId(), 2},
		{opensplunk.AuditAction_AUDIT_ACTION_INDEX_CREATE, keepIndex.GetIndexId(), 1},
		{opensplunk.AuditAction_AUDIT_ACTION_INDEX_DELETE_DATA, dataIndex.GetIndexId(), 6},
		{opensplunk.AuditAction_AUDIT_ACTION_INDEX_ARCHIVE, dataIndex.GetIndexId(), 5},
		{opensplunk.AuditAction_AUDIT_ACTION_INDEX_ACTIVATE, dataIndex.GetIndexId(), 4},
		{opensplunk.AuditAction_AUDIT_ACTION_INDEX_ARCHIVE, dataIndex.GetIndexId(), 3},
		{opensplunk.AuditAction_AUDIT_ACTION_INDEX_UPDATE, dataIndex.GetIndexId(), 2},
		{opensplunk.AuditAction_AUDIT_ACTION_INDEX_CREATE, dataIndex.GetIndexId(), 1},
	}
	assertRuntimeIndexAuditEvents(t, listedEvents, expectations)
	for _, canary := range payloadCanaries {
		if bytes.Contains(firstPagePayload, []byte(canary)) ||
			bytes.Contains(secondPagePayload, []byte(canary)) {
			t.Fatalf("audit API response contains request payload canary %q", canary)
		}
	}

	persistedTargetKind := audit.TargetKindIndex
	persistedPage, err := secondStores.auditEvents.List(
		ctx,
		runtimeIndexAuditTenantID,
		audit.ListRequest{
			PageSize:     20,
			TargetKind:   &persistedTargetKind,
			IncludeTotal: true,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(persistedPage.Events) != len(expectations) ||
		persistedPage.TotalSize == nil ||
		*persistedPage.TotalSize != uint64(len(expectations)) ||
		!persistedPage.TotalSizeExact ||
		persistedPage.NextPageToken != "" {
		t.Fatalf("persisted audit page = %+v", persistedPage)
	}
	for _, event := range persistedPage.Events {
		if event.TenantID != runtimeIndexAuditTenantID ||
			event.Actor != (audit.Actor{
				Kind: audit.ActorKindBrowser,
				ID:   runtimeIndexAuditOwnerID,
				Role: audit.ActorRoleAdministrator,
			}) {
			t.Fatalf("persisted audit scope = %+v", event)
		}
	}
	otherTenantPage, err := secondStores.auditEvents.List(
		ctx,
		"tenant-b",
		audit.ListRequest{PageSize: 20},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(otherTenantPage.Events) != 0 {
		t.Fatalf("other-tenant audit events = %d, want 0", len(otherTenantPage.Events))
	}
	assertRuntimeIndexAuditRowsOmitPayload(
		t,
		ctx,
		secondDatabase,
		payloadCanaries,
	)

	wrongBearerToken := bytes.Repeat(
		[]byte("w"),
		auth.MinimumBrowserBearerTokenBytes,
	)
	wrongScopeHandler := newRuntimeIndexAuditHandlerForTest(
		t,
		secondDatabase,
		secondAdministration,
		secondStores.auditEvents,
		newRuntimeIndexAuditAuthenticator(t, wrongBearerToken, "tenant-b"),
		nil,
	)
	wrongScopeResponse := postRuntimeAppProto(
		t,
		wrongScopeHandler,
		"/api/audit/events/list",
		&opensplunk.ListAuditEventsRequest{},
		wrongBearerToken,
	)
	if wrongScopeResponse.Code != http.StatusForbidden {
		t.Fatalf(
			"wrong-tenant audit list status = %d, body = %s",
			wrongScopeResponse.Code,
			wrongScopeResponse.Body.String(),
		)
	}
}

func TestRuntimeIndexAdministrationAuditFailureRollsBack(t *testing.T) {
	ctx := context.Background()
	database, err := control.Open(
		ctx,
		filepath.Join(t.TempDir(), "control.db"),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := database.Close(); err != nil {
			t.Errorf("close control database: %v", err)
		}
	})
	rejection := errors.New("private-audit-backend-failure-5173")
	appender := &rejectingRuntimeIndexAuditAppender{err: rejection}
	administration, err := newRuntimeIndexAdministration(
		database,
		runtimeIndexAuditTenantID,
		appender,
	)
	if err != nil {
		t.Fatal(err)
	}
	bearerToken := bytes.Repeat(
		[]byte("f"),
		auth.MinimumBrowserBearerTokenBytes,
	)
	handler := newRuntimeIndexAuditHandlerForTest(
		t,
		database,
		administration,
		nil,
		newRuntimeIndexAuditAuthenticator(
			t,
			bearerToken,
			runtimeIndexAuditTenantID,
		),
		nil,
	)
	indexName := "runtime-audit-failure-rollback"
	definitionCanary := "rollback-request-payload-must-not-appear-6284"
	response := postRuntimeAppProto(
		t,
		handler,
		"/api/indexes/create",
		&opensplunk.CreateIndexRequest{
			Definition: runtimeIndexAuditDefinition(
				indexName,
				"Rollback audit mutation",
				definitionCanary,
			),
		},
		bearerToken,
	)
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf(
			"audit failure create status = %d, body = %s",
			response.Code,
			response.Body.String(),
		)
	}
	if strings.Contains(response.Body.String(), rejection.Error()) ||
		strings.Contains(response.Body.String(), definitionCanary) {
		t.Fatalf("audit failure response disclosed private detail: %s", response.Body.String())
	}
	if _, err := database.GetIndexByName(ctx, indexName); !errors.Is(err, control.ErrNotFound) {
		t.Fatalf("GetIndexByName after audit failure error = %v, want ErrNotFound", err)
	}
	var auditRows int64
	if err := database.SQLDB().QueryRowContext(
		ctx,
		"SELECT COUNT(*) FROM audit_events",
	).Scan(&auditRows); err != nil {
		t.Fatal(err)
	}
	if auditRows != 0 {
		t.Fatalf("audit rows after rejected mutation = %d, want 0", auditRows)
	}
	if appender.calls != 1 || !appender.transactionPresent ||
		appender.tenantID != runtimeIndexAuditTenantID ||
		appender.actor != (audit.Actor{
			Kind: audit.ActorKindBrowser,
			ID:   runtimeIndexAuditOwnerID,
			Role: audit.ActorRoleAdministrator,
		}) ||
		appender.event.Action != control.IndexMutationAuditActionCreate ||
		appender.event.IndexID == "" ||
		appender.event.IndexVersion != 1 {
		t.Fatalf("rejected audit append = %+v", appender)
	}
}

type runtimeIndexAuditExpectation struct {
	action  opensplunk.AuditAction
	indexID string
	version uint64
}

func assertRuntimeIndexAuditEvents(
	t *testing.T,
	events []*opensplunk.AuditEvent,
	expectations []runtimeIndexAuditExpectation,
) {
	t.Helper()
	if len(events) != len(expectations) {
		t.Fatalf("audit events = %d, want %d", len(events), len(expectations))
	}
	for index, expectation := range expectations {
		event := events[index]
		wantSequence := uint64(len(expectations) - index)
		if event.GetSequence() != wantSequence ||
			event.GetActorKind() != opensplunk.AuditActorKind_AUDIT_ACTOR_KIND_BROWSER ||
			event.GetActorId() != runtimeIndexAuditOwnerID ||
			event.GetActorRole() != opensplunk.AuditActorRole_AUDIT_ACTOR_ROLE_ADMINISTRATOR ||
			event.GetAction() != expectation.action ||
			event.GetTargetKind() != opensplunk.AuditTargetKind_AUDIT_TARGET_KIND_INDEX ||
			event.GetTargetId() != expectation.indexID ||
			event.GetTargetVersion() != expectation.version ||
			event.GetOccurredAt() == nil ||
			event.GetOccurredAt().CheckValid() != nil {
			t.Fatalf("audit event %d = %+v, want %+v", index, event, expectation)
		}
	}
}

func assertRuntimeIndexAuditRowsOmitPayload(
	t *testing.T,
	ctx context.Context,
	database *control.DB,
	canaries []string,
) {
	t.Helper()
	rows, err := database.SQLDB().QueryContext(ctx, `
		SELECT tenant_id, actor_kind, actor_id, actor_role,
		       action, target_kind, target_id
		FROM audit_events
		ORDER BY sequence ASC
	`)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			t.Errorf("close audit row scan: %v", err)
		}
	}()
	for rows.Next() {
		var values [7]string
		if err := rows.Scan(
			&values[0],
			&values[1],
			&values[2],
			&values[3],
			&values[4],
			&values[5],
			&values[6],
		); err != nil {
			t.Fatal(err)
		}
		projection := strings.Join(values[:], "\x00")
		for _, canary := range canaries {
			if strings.Contains(projection, canary) {
				t.Fatalf("persisted audit projection contains request payload canary %q", canary)
			}
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
}

func newRuntimeIndexAuditAuthenticator(
	t *testing.T,
	bearerToken []byte,
	tenantID string,
) auth.BrowserAuthenticator {
	t.Helper()
	authenticator, err := auth.NewBearerTokenAuthenticator(
		bearerToken,
		tenantID,
		runtimeIndexAuditOwnerID,
		auth.BrowserRoleAdministrator,
	)
	if err != nil {
		t.Fatal(err)
	}
	return authenticator
}

func newRuntimeIndexAuditHandlerForTest(
	t *testing.T,
	database *control.DB,
	administration *control.AuditedIndexAdministration,
	auditEvents server.AuditEvents,
	authenticator auth.BrowserAuthenticator,
	waker server.IndexDataDeletionWaker,
) *server.Handler {
	t.Helper()
	config := runtimeServerConfig()
	config.Indexes = database
	config.IndexAdmin = administration
	config.AuditEvents = auditEvents
	config.BrowserAuthenticator = authenticator
	config.TenantID = runtimeIndexAuditTenantID
	config.OwnerID = runtimeIndexAuditOwnerID
	config.AdministrativeAllowedHosts = []string{"127.0.0.1"}
	if waker != nil {
		config.IndexDataDeletionAdmission = administration
		config.IndexDataDeletionWaker = waker
	}
	handler, err := server.NewHandler(config)
	if err != nil {
		t.Fatal(err)
	}
	return handler
}

func createRuntimeIndexForAudit(
	t *testing.T,
	handler http.Handler,
	bearerToken []byte,
	name string,
	displayName string,
	description string,
) *opensplunk.Index {
	t.Helper()
	var response opensplunk.CreateIndexResponse
	postRuntimeProtoOK(
		t,
		handler,
		"/api/indexes/create",
		&opensplunk.CreateIndexRequest{
			Definition: runtimeIndexAuditDefinition(
				name,
				displayName,
				description,
			),
		},
		&response,
		bearerToken,
	)
	if response.GetIndex().GetIndexId() == "" ||
		response.GetIndex().GetVersion() != 1 {
		t.Fatalf("created index = %+v", response.GetIndex())
	}
	return response.GetIndex()
}

func updateRuntimeIndexDescriptionForAudit(
	t *testing.T,
	handler http.Handler,
	bearerToken []byte,
	index *opensplunk.Index,
	description string,
) *opensplunk.Index {
	t.Helper()
	var response opensplunk.UpdateIndexResponse
	postRuntimeProtoOK(
		t,
		handler,
		"/api/indexes/update",
		&opensplunk.UpdateIndexRequest{
			Selector:        runtimeIndexAuditSelector(index.GetIndexId()),
			ExpectedVersion: index.GetVersion(),
			Definition: &opensplunk.IndexDefinition{
				Description: &description,
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"description"}},
		},
		&response,
		bearerToken,
	)
	if response.GetIndex().GetVersion() != index.GetVersion()+1 {
		t.Fatalf("updated index = %+v", response.GetIndex())
	}
	return response.GetIndex()
}

func setRuntimeIndexStateForAudit(
	t *testing.T,
	handler http.Handler,
	bearerToken []byte,
	index *opensplunk.Index,
	state opensplunk.IndexState,
) *opensplunk.Index {
	t.Helper()
	var response opensplunk.SetIndexStateResponse
	postRuntimeProtoOK(
		t,
		handler,
		"/api/indexes/state/set",
		&opensplunk.SetIndexStateRequest{
			Selector:        runtimeIndexAuditSelector(index.GetIndexId()),
			ExpectedVersion: index.GetVersion(),
			State:           state,
		},
		&response,
		bearerToken,
	)
	if response.GetIndex().GetVersion() != index.GetVersion()+1 ||
		response.GetIndex().GetState() != state {
		t.Fatalf("state-updated index = %+v", response.GetIndex())
	}
	return response.GetIndex()
}

func deleteRuntimeIndexForAudit(
	t *testing.T,
	handler http.Handler,
	bearerToken []byte,
	request *opensplunk.DeleteIndexRequest,
) *opensplunk.DeleteIndexResponse {
	t.Helper()
	var response opensplunk.DeleteIndexResponse
	postRuntimeProtoOK(
		t,
		handler,
		"/api/indexes/delete",
		request,
		&response,
		bearerToken,
	)
	return &response
}

func runtimeIndexAuditDefinition(
	name string,
	displayName string,
	description string,
) *opensplunk.IndexDefinition {
	return &opensplunk.IndexDefinition{
		Name:            name,
		DisplayName:     displayName,
		Description:     &description,
		IngestionAccess: opensplunk.IndexAccessState_INDEX_ACCESS_STATE_ENABLED,
		SearchAccess:    opensplunk.IndexAccessState_INDEX_ACCESS_STATE_ENABLED,
	}
}

func runtimeIndexAuditSelector(indexID string) *opensplunk.IndexSelector {
	return &opensplunk.IndexSelector{
		Selector: &opensplunk.IndexSelector_IndexId{IndexId: indexID},
	}
}

type runtimeIndexAuditWaker struct{}

func (*runtimeIndexAuditWaker) Wake() {}

type rejectingRuntimeIndexAuditAppender struct {
	err                error
	calls              int
	transactionPresent bool
	tenantID           string
	actor              audit.Actor
	event              control.IndexMutationAuditEvent
}

func (appender *rejectingRuntimeIndexAuditAppender) AppendIndexMutationInTransaction(
	ctx context.Context,
	transaction *gorm.DB,
	tenantID string,
	event control.IndexMutationAuditEvent,
) error {
	appender.calls++
	appender.transactionPresent = transaction != nil &&
		transaction.Statement != nil
	appender.tenantID = strings.Clone(tenantID)
	appender.actor, _ = audit.ActorFromContext(ctx)
	appender.event = event
	return appender.err
}
