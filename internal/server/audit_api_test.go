package server

import (
	"bytes"
	"context"
	"errors"
	"math"
	"net/http"
	"net/http/httptest"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/audit"
	"github.com/Suhaibinator/open-splunk/internal/auth"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"google.golang.org/protobuf/proto"
)

type fakeAuditEvents struct {
	mu       sync.Mutex
	calls    int
	tenantID string
	request  audit.ListRequest
	page     audit.ListPage
	err      error
}

func (service *fakeAuditEvents) List(
	ctx context.Context,
	tenantID string,
	request audit.ListRequest,
) (audit.ListPage, error) {
	service.mu.Lock()
	defer service.mu.Unlock()
	service.calls++
	service.tenantID = tenantID
	service.request = request
	if err := ctx.Err(); err != nil {
		return audit.ListPage{}, err
	}
	return service.page, service.err
}

func (service *fakeAuditEvents) snapshot() (int, string, audit.ListRequest) {
	service.mu.Lock()
	defer service.mu.Unlock()
	return service.calls, service.tenantID, service.request
}

func TestIndexAuditProtoTaxonomyRoundTripsAndAcceptsCompleteFilterSet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		action  audit.Action
		proto   opensplunk.AuditAction
		version uint64
	}{
		{audit.ActionIndexCreate, opensplunk.AuditAction_AUDIT_ACTION_INDEX_CREATE, 1},
		{audit.ActionIndexUpdate, opensplunk.AuditAction_AUDIT_ACTION_INDEX_UPDATE, 2},
		{audit.ActionIndexActivate, opensplunk.AuditAction_AUDIT_ACTION_INDEX_ACTIVATE, 2},
		{audit.ActionIndexArchive, opensplunk.AuditAction_AUDIT_ACTION_INDEX_ARCHIVE, 2},
		{audit.ActionIndexDeleteKeepData, opensplunk.AuditAction_AUDIT_ACTION_INDEX_DELETE_KEEP_DATA, 2},
		{audit.ActionIndexDeleteData, opensplunk.AuditAction_AUDIT_ACTION_INDEX_DELETE_DATA, 3},
	}
	for _, testCase := range tests {
		t.Run(string(testCase.action), func(t *testing.T) {
			t.Parallel()
			fromProto, ok := auditActionFromProto(testCase.proto)
			if !ok || fromProto != testCase.action {
				t.Fatalf("auditActionFromProto(%v) = (%q, %t)", testCase.proto, fromProto, ok)
			}
			toProto, ok := auditActionToProto(testCase.action)
			if !ok || toProto != testCase.proto {
				t.Fatalf("auditActionToProto(%q) = (%v, %t)", testCase.action, toProto, ok)
			}
			message, err := auditEventToProto(audit.Event{
				Sequence:   1,
				TenantID:   "tenant",
				OccurredAt: time.Date(2026, time.August, 3, 20, 21, 22, 123456000, time.UTC),
				Actor: audit.Actor{
					Kind: audit.ActorKindSystem,
					ID:   "open-splunk-server",
					Role: audit.ActorRoleSystem,
				},
				Action:        testCase.action,
				TargetKind:    audit.TargetKindIndex,
				TargetID:      "events",
				TargetVersion: testCase.version,
			}, "tenant")
			if err != nil {
				t.Fatalf("auditEventToProto(%q): %v", testCase.action, err)
			}
			if message.GetAction() != testCase.proto ||
				message.GetTargetKind() != opensplunk.AuditTargetKind_AUDIT_TARGET_KIND_INDEX {
				t.Fatalf("index audit proto = %+v", message)
			}
		})
	}

	indexTarget := opensplunk.AuditTargetKind_AUDIT_TARGET_KIND_INDEX
	allActions := []opensplunk.AuditAction{
		opensplunk.AuditAction_AUDIT_ACTION_INGESTION_TOKEN_CREATE,
		opensplunk.AuditAction_AUDIT_ACTION_INGESTION_TOKEN_UPDATE,
		opensplunk.AuditAction_AUDIT_ACTION_INGESTION_TOKEN_REVOKE,
		opensplunk.AuditAction_AUDIT_ACTION_INDEX_CREATE,
		opensplunk.AuditAction_AUDIT_ACTION_INDEX_UPDATE,
		opensplunk.AuditAction_AUDIT_ACTION_INDEX_ACTIVATE,
		opensplunk.AuditAction_AUDIT_ACTION_INDEX_ARCHIVE,
		opensplunk.AuditAction_AUDIT_ACTION_INDEX_DELETE_KEEP_DATA,
		opensplunk.AuditAction_AUDIT_ACTION_INDEX_DELETE_DATA,
		opensplunk.AuditAction_AUDIT_ACTION_APP_CREATE,
		opensplunk.AuditAction_AUDIT_ACTION_APP_UPDATE,
		opensplunk.AuditAction_AUDIT_ACTION_APP_ACTIVATE,
		opensplunk.AuditAction_AUDIT_ACTION_APP_ARCHIVE,
		opensplunk.AuditAction_AUDIT_ACTION_APP_DELETE,
		opensplunk.AuditAction_AUDIT_ACTION_SAVED_SEARCH_CREATE,
		opensplunk.AuditAction_AUDIT_ACTION_SAVED_SEARCH_UPDATE,
		opensplunk.AuditAction_AUDIT_ACTION_SAVED_SEARCH_DUPLICATE,
		opensplunk.AuditAction_AUDIT_ACTION_SAVED_SEARCH_DELETE,
		opensplunk.AuditAction_AUDIT_ACTION_KNOWLEDGE_OBJECT_CREATE,
		opensplunk.AuditAction_AUDIT_ACTION_KNOWLEDGE_OBJECT_UPDATE,
		opensplunk.AuditAction_AUDIT_ACTION_KNOWLEDGE_OBJECT_SCOPE_CHANGE,
		opensplunk.AuditAction_AUDIT_ACTION_KNOWLEDGE_OBJECT_ENABLE,
		opensplunk.AuditAction_AUDIT_ACTION_KNOWLEDGE_OBJECT_DISABLE,
		opensplunk.AuditAction_AUDIT_ACTION_KNOWLEDGE_OBJECT_DELETE,
		opensplunk.AuditAction_AUDIT_ACTION_SERVER_SETTINGS_UPDATE,
		opensplunk.AuditAction_AUDIT_ACTION_LOOKUP_CREATE,
		opensplunk.AuditAction_AUDIT_ACTION_LOOKUP_REPLACE,
		opensplunk.AuditAction_AUDIT_ACTION_LOOKUP_ENABLE,
		opensplunk.AuditAction_AUDIT_ACTION_LOOKUP_DISABLE,
		opensplunk.AuditAction_AUDIT_ACTION_LOOKUP_DELETE,
	}
	service := &fakeAuditEvents{}
	handler := newAuditTestHandler(t, service)
	response := postAuthenticatedAudit(t, handler, &opensplunk.ListAuditEventsRequest{
		ActionFilters:    allActions,
		TargetKindFilter: &indexTarget,
	})
	if response.Code != http.StatusOK {
		t.Fatalf("all-action filter status = %d, body = %s", response.Code, response.Body.String())
	}
	calls, _, request := service.snapshot()
	if calls != 1 || len(request.ActionFilters) != audit.MaximumActionFilters ||
		request.TargetKind == nil || *request.TargetKind != audit.TargetKindIndex {
		t.Fatalf("complete audit filter call = %d/%+v", calls, request)
	}
}

func TestServerSettingsAuditProtoTaxonomyRoundTrips(t *testing.T) {
	t.Parallel()

	fromAction, ok := auditActionFromProto(
		opensplunk.AuditAction_AUDIT_ACTION_SERVER_SETTINGS_UPDATE,
	)
	if !ok || fromAction != audit.ActionServerSettingsUpdate {
		t.Fatalf("auditActionFromProto(server settings update) = (%q, %t)", fromAction, ok)
	}
	toAction, ok := auditActionToProto(audit.ActionServerSettingsUpdate)
	if !ok || toAction != opensplunk.AuditAction_AUDIT_ACTION_SERVER_SETTINGS_UPDATE {
		t.Fatalf("auditActionToProto(server settings update) = (%v, %t)", toAction, ok)
	}

	fromTarget, ok := auditTargetKindFromProto(
		opensplunk.AuditTargetKind_AUDIT_TARGET_KIND_SERVER_SETTINGS,
	)
	if !ok || fromTarget != audit.TargetKindServerSettings {
		t.Fatalf("auditTargetKindFromProto(server settings) = (%q, %t)", fromTarget, ok)
	}
	toTarget, ok := auditTargetKindToProto(audit.TargetKindServerSettings)
	if !ok || toTarget != opensplunk.AuditTargetKind_AUDIT_TARGET_KIND_SERVER_SETTINGS {
		t.Fatalf("auditTargetKindToProto(server settings) = (%v, %t)", toTarget, ok)
	}

	message, err := auditEventToProto(audit.Event{
		Sequence:   1,
		TenantID:   "tenant",
		OccurredAt: time.Date(2026, time.August, 3, 20, 21, 22, 123456000, time.UTC),
		Actor: audit.Actor{
			Kind: audit.ActorKindBrowser,
			ID:   "administrator",
			Role: audit.ActorRoleAdministrator,
		},
		Action:        audit.ActionServerSettingsUpdate,
		TargetKind:    audit.TargetKindServerSettings,
		TargetID:      "search-limits",
		TargetVersion: 1,
	}, "tenant")
	if err != nil {
		t.Fatalf("auditEventToProto(server settings update): %v", err)
	}
	if message.GetAction() != opensplunk.AuditAction_AUDIT_ACTION_SERVER_SETTINGS_UPDATE ||
		message.GetTargetKind() != opensplunk.AuditTargetKind_AUDIT_TARGET_KIND_SERVER_SETTINGS {
		t.Fatalf("server-settings audit proto = %+v", message)
	}
}

func TestKnowledgeAuditProtoTaxonomyAndMetadataRoundTrip(t *testing.T) {
	t.Parallel()

	for _, testCase := range []struct {
		action  audit.Action
		proto   opensplunk.AuditAction
		version uint64
	}{
		{audit.ActionKnowledgeObjectCreate, opensplunk.AuditAction_AUDIT_ACTION_KNOWLEDGE_OBJECT_CREATE, 1},
		{audit.ActionKnowledgeObjectUpdate, opensplunk.AuditAction_AUDIT_ACTION_KNOWLEDGE_OBJECT_UPDATE, 2},
		{audit.ActionKnowledgeObjectScopeChange, opensplunk.AuditAction_AUDIT_ACTION_KNOWLEDGE_OBJECT_SCOPE_CHANGE, 2},
		{audit.ActionKnowledgeObjectEnable, opensplunk.AuditAction_AUDIT_ACTION_KNOWLEDGE_OBJECT_ENABLE, 2},
		{audit.ActionKnowledgeObjectDisable, opensplunk.AuditAction_AUDIT_ACTION_KNOWLEDGE_OBJECT_DISABLE, 2},
		{audit.ActionKnowledgeObjectDelete, opensplunk.AuditAction_AUDIT_ACTION_KNOWLEDGE_OBJECT_DELETE, 2},
	} {
		t.Run(string(testCase.action), func(t *testing.T) {
			t.Parallel()
			fromProto, ok := auditActionFromProto(testCase.proto)
			if !ok || fromProto != testCase.action {
				t.Fatalf("auditActionFromProto(%v) = (%q, %t)", testCase.proto, fromProto, ok)
			}
			toProto, ok := auditActionToProto(testCase.action)
			if !ok || toProto != testCase.proto {
				t.Fatalf("auditActionToProto(%q) = (%v, %t)", testCase.action, toProto, ok)
			}
			message, err := auditEventToProto(audit.Event{
				Sequence:   1,
				TenantID:   "tenant",
				OccurredAt: time.Date(2026, time.August, 3, 20, 21, 22, 123456000, time.UTC),
				Actor: audit.Actor{
					Kind: audit.ActorKindBrowser,
					ID:   "administrator",
					Role: audit.ActorRoleAdministrator,
				},
				Action:        testCase.action,
				TargetKind:    audit.TargetKindKnowledgeObject,
				TargetID:      "ko_AAAAAAAAAAAAAAAAAAAAAA",
				TargetVersion: testCase.version,
				KnowledgeObject: audit.KnowledgeObjectMetadata{
					AppID:        "app_AAAAAAAAAAAAAAAAAAAAAA",
					ObjectType:   audit.KnowledgeObjectTypeFieldExtraction,
					SharingScope: audit.KnowledgeSharingScopeApp,
				},
			}, "tenant")
			if err != nil {
				t.Fatalf("auditEventToProto(%q): %v", testCase.action, err)
			}
			if message.GetAction() != testCase.proto ||
				message.GetTargetKind() != opensplunk.AuditTargetKind_AUDIT_TARGET_KIND_KNOWLEDGE_OBJECT ||
				message.AppId == nil || message.GetAppId() != "app_AAAAAAAAAAAAAAAAAAAAAA" ||
				message.ObjectType == nil || message.GetObjectType() != opensplunk.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_FIELD_EXTRACTION ||
				message.SharingScope == nil || message.GetSharingScope() != opensplunk.SharingScope_SHARING_SCOPE_APP {
				t.Fatalf("knowledge-object audit proto = %+v", message)
			}
		})
	}

	fromProto, ok := auditTargetKindFromProto(
		opensplunk.AuditTargetKind_AUDIT_TARGET_KIND_KNOWLEDGE_OBJECT,
	)
	if !ok || fromProto != audit.TargetKindKnowledgeObject {
		t.Fatalf("auditTargetKindFromProto(knowledge object) = (%q, %t)", fromProto, ok)
	}
	toProto, ok := auditTargetKindToProto(audit.TargetKindKnowledgeObject)
	if !ok || toProto != opensplunk.AuditTargetKind_AUDIT_TARGET_KIND_KNOWLEDGE_OBJECT {
		t.Fatalf("auditTargetKindToProto(knowledge object) = (%v, %t)", toProto, ok)
	}
	legacy, err := auditEventToProto(audit.Event{
		Sequence: 1, TenantID: "tenant",
		OccurredAt: time.Date(2026, time.August, 3, 20, 21, 22, 123456000, time.UTC),
		Actor: audit.Actor{
			Kind: audit.ActorKindBrowser, ID: "ordinary-user", Role: audit.ActorRoleUser,
		},
		Action: audit.ActionSavedSearchUpdate, TargetKind: audit.TargetKindSavedSearch,
		TargetID: "saved-search-a", TargetVersion: 2,
	}, "tenant")
	if err != nil {
		t.Fatalf("auditEventToProto(legacy): %v", err)
	}
	if legacy.AppId != nil || legacy.ObjectType != nil || legacy.SharingScope != nil {
		t.Fatalf("legacy audit proto has knowledge metadata: %+v", legacy)
	}
}

func TestAppAuditProtoTaxonomyRoundTrips(t *testing.T) {
	t.Parallel()

	for _, testCase := range []struct {
		action  audit.Action
		proto   opensplunk.AuditAction
		version uint64
	}{
		{audit.ActionAppCreate, opensplunk.AuditAction_AUDIT_ACTION_APP_CREATE, 1},
		{audit.ActionAppUpdate, opensplunk.AuditAction_AUDIT_ACTION_APP_UPDATE, 2},
		{audit.ActionAppActivate, opensplunk.AuditAction_AUDIT_ACTION_APP_ACTIVATE, 3},
		{audit.ActionAppArchive, opensplunk.AuditAction_AUDIT_ACTION_APP_ARCHIVE, 4},
		{audit.ActionAppDelete, opensplunk.AuditAction_AUDIT_ACTION_APP_DELETE, 4},
	} {
		t.Run(string(testCase.action), func(t *testing.T) {
			t.Parallel()
			fromProto, ok := auditActionFromProto(testCase.proto)
			if !ok || fromProto != testCase.action {
				t.Fatalf("auditActionFromProto(%v) = (%q, %t)", testCase.proto, fromProto, ok)
			}
			toProto, ok := auditActionToProto(testCase.action)
			if !ok || toProto != testCase.proto {
				t.Fatalf("auditActionToProto(%q) = (%v, %t)", testCase.action, toProto, ok)
			}
			message, err := auditEventToProto(audit.Event{
				Sequence:   1,
				TenantID:   "tenant",
				OccurredAt: time.Date(2026, time.August, 3, 20, 21, 22, 123456000, time.UTC),
				Actor: audit.Actor{
					Kind: audit.ActorKindBrowser,
					ID:   "administrator",
					Role: audit.ActorRoleAdministrator,
				},
				Action:        testCase.action,
				TargetKind:    audit.TargetKindApp,
				TargetID:      "app-observability",
				TargetVersion: testCase.version,
			}, "tenant")
			if err != nil {
				t.Fatalf("auditEventToProto(%q): %v", testCase.action, err)
			}
			if message.GetAction() != testCase.proto ||
				message.GetTargetKind() != opensplunk.AuditTargetKind_AUDIT_TARGET_KIND_APP ||
				message.GetTargetId() != "app-observability" ||
				message.GetTargetVersion() != testCase.version {
				t.Fatalf("app audit proto = %+v", message)
			}
		})
	}

	fromProto, ok := auditTargetKindFromProto(
		opensplunk.AuditTargetKind_AUDIT_TARGET_KIND_APP,
	)
	if !ok || fromProto != audit.TargetKindApp {
		t.Fatalf("auditTargetKindFromProto(app) = (%q, %t)", fromProto, ok)
	}
	toProto, ok := auditTargetKindToProto(audit.TargetKindApp)
	if !ok || toProto != opensplunk.AuditTargetKind_AUDIT_TARGET_KIND_APP {
		t.Fatalf("auditTargetKindToProto(app) = (%v, %t)", toProto, ok)
	}
}

func TestSavedSearchAuditProtoTaxonomyRoundTrips(t *testing.T) {
	t.Parallel()

	for _, testCase := range []struct {
		action  audit.Action
		proto   opensplunk.AuditAction
		version uint64
	}{
		{audit.ActionSavedSearchCreate, opensplunk.AuditAction_AUDIT_ACTION_SAVED_SEARCH_CREATE, 1},
		{audit.ActionSavedSearchUpdate, opensplunk.AuditAction_AUDIT_ACTION_SAVED_SEARCH_UPDATE, 2},
		{audit.ActionSavedSearchDuplicate, opensplunk.AuditAction_AUDIT_ACTION_SAVED_SEARCH_DUPLICATE, 1},
		{audit.ActionSavedSearchDelete, opensplunk.AuditAction_AUDIT_ACTION_SAVED_SEARCH_DELETE, 1},
	} {
		t.Run(string(testCase.action), func(t *testing.T) {
			t.Parallel()
			fromProto, ok := auditActionFromProto(testCase.proto)
			if !ok || fromProto != testCase.action {
				t.Fatalf("auditActionFromProto(%v) = (%q, %t)", testCase.proto, fromProto, ok)
			}
			toProto, ok := auditActionToProto(testCase.action)
			if !ok || toProto != testCase.proto {
				t.Fatalf("auditActionToProto(%q) = (%v, %t)", testCase.action, toProto, ok)
			}
			message, err := auditEventToProto(audit.Event{
				Sequence:   1,
				TenantID:   "tenant",
				OccurredAt: time.Date(2026, time.August, 3, 20, 21, 22, 123456000, time.UTC),
				Actor: audit.Actor{
					Kind: audit.ActorKindBrowser,
					ID:   "single-user",
					Role: audit.ActorRoleUser,
				},
				Action:        testCase.action,
				TargetKind:    audit.TargetKindSavedSearch,
				TargetID:      "saved-search-observability",
				TargetVersion: testCase.version,
			}, "tenant")
			if err != nil {
				t.Fatalf("auditEventToProto(%q): %v", testCase.action, err)
			}
			if message.GetAction() != testCase.proto ||
				message.GetTargetKind() != opensplunk.AuditTargetKind_AUDIT_TARGET_KIND_SAVED_SEARCH ||
				message.GetActorRole() != opensplunk.AuditActorRole_AUDIT_ACTOR_ROLE_USER ||
				message.GetTargetId() != "saved-search-observability" ||
				message.GetTargetVersion() != testCase.version {
				t.Fatalf("saved-search audit proto = %+v", message)
			}
		})
	}

	fromProto, ok := auditTargetKindFromProto(
		opensplunk.AuditTargetKind_AUDIT_TARGET_KIND_SAVED_SEARCH,
	)
	if !ok || fromProto != audit.TargetKindSavedSearch {
		t.Fatalf("auditTargetKindFromProto(saved search) = (%q, %t)", fromProto, ok)
	}
	toProto, ok := auditTargetKindToProto(audit.TargetKindSavedSearch)
	if !ok || toProto != opensplunk.AuditTargetKind_AUDIT_TARGET_KIND_SAVED_SEARCH {
		t.Fatalf("auditTargetKindToProto(saved search) = (%v, %t)", toProto, ok)
	}
}

func TestAuditEventListUsesAuthenticatedTenantAndProjectsBoundedPage(t *testing.T) {
	t.Parallel()

	anchor := time.Date(2026, time.August, 3, 20, 21, 22, 123456000, time.UTC)
	total := uint64(3)
	service := &fakeAuditEvents{page: audit.ListPage{
		Events: []audit.Event{
			{
				Sequence:   3,
				TenantID:   browserGateTenantID,
				OccurredAt: anchor,
				Actor: audit.Actor{
					Kind: audit.ActorKindBrowser,
					ID:   browserGateOwnerID,
					Role: audit.ActorRoleAdministrator,
				},
				Action:        audit.ActionIngestionTokenRevoke,
				TargetKind:    audit.TargetKindIngestionToken,
				TargetID:      "token-3",
				TargetVersion: 4,
			},
			{
				Sequence:   2,
				TenantID:   browserGateTenantID,
				OccurredAt: anchor.Add(-time.Second),
				Actor: audit.Actor{
					Kind: audit.ActorKindBrowser,
					ID:   browserGateOwnerID,
					Role: audit.ActorRoleAdministrator,
				},
				Action:        audit.ActionIngestionTokenCreate,
				TargetKind:    audit.TargetKindIngestionToken,
				TargetID:      "token-2",
				TargetVersion: 1,
			},
		},
		NextPageToken:  "signed-next-page",
		TotalSize:      &total,
		TotalSizeExact: true,
	}}
	handler := newAuditTestHandler(t, service)
	actorID := browserGateOwnerID
	targetKind := opensplunk.AuditTargetKind_AUDIT_TARGET_KIND_INGESTION_TOKEN
	request := &opensplunk.ListAuditEventsRequest{
		Page: &opensplunk.PageRequest{
			PageSize:         new(uint32(2)),
			IncludeTotalSize: true,
		},
		ActionFilters: []opensplunk.AuditAction{
			opensplunk.AuditAction_AUDIT_ACTION_INGESTION_TOKEN_CREATE,
			opensplunk.AuditAction_AUDIT_ACTION_INGESTION_TOKEN_REVOKE,
		},
		ActorIdFilter:    &actorID,
		TargetKindFilter: &targetKind,
	}
	request.ProtoReflect().SetUnknown(futureProtobufField("future-audit-filter"))
	response := postAuthenticatedAudit(t, handler, request)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}

	calls, tenantID, gotRequest := service.snapshot()
	if calls != 1 || tenantID != browserGateTenantID ||
		gotRequest.PageSize != 2 || !gotRequest.IncludeTotal ||
		gotRequest.ActorID == nil || *gotRequest.ActorID != actorID ||
		gotRequest.TargetKind == nil || *gotRequest.TargetKind != audit.TargetKindIngestionToken ||
		!slices.Equal(gotRequest.ActionFilters, []audit.Action{
			audit.ActionIngestionTokenCreate,
			audit.ActionIngestionTokenRevoke,
		}) {
		t.Fatalf("audit list call = %d/%q/%#v", calls, tenantID, gotRequest)
	}

	var decoded opensplunk.ListAuditEventsResponse
	unmarshalResponse(t, response, &decoded)
	if len(decoded.GetAuditEvents()) != 2 ||
		decoded.GetPage().GetNextPageToken() != "signed-next-page" ||
		decoded.GetPage().GetTotalSize() != total ||
		!decoded.GetPage().GetTotalSizeExact() {
		t.Fatalf("audit response = %#v", &decoded)
	}
	first := decoded.GetAuditEvents()[0]
	if first.GetSequence() != 3 ||
		!first.GetOccurredAt().AsTime().Equal(anchor) ||
		first.GetActorKind() != opensplunk.AuditActorKind_AUDIT_ACTOR_KIND_BROWSER ||
		first.GetActorId() != browserGateOwnerID ||
		first.GetActorRole() != opensplunk.AuditActorRole_AUDIT_ACTOR_ROLE_ADMINISTRATOR ||
		first.GetAction() != opensplunk.AuditAction_AUDIT_ACTION_INGESTION_TOKEN_REVOKE ||
		first.GetTargetKind() != opensplunk.AuditTargetKind_AUDIT_TARGET_KIND_INGESTION_TOKEN ||
		first.GetTargetId() != "token-3" || first.GetTargetVersion() != 4 {
		t.Fatalf("first audit event = %#v", first)
	}
	if encoded := response.Body.String(); containsAny(encoded, []string{"plaintext_token", "token_digest", "Authorization", adminIntegrationBearerToken}) {
		t.Fatalf("audit response exposed forbidden credential material: %q", encoded)
	}
}

func TestAuditEventHTTPVerticalCapturesAuthenticatedTokenCreationWithoutSecrets(
	t *testing.T,
) {
	t.Parallel()

	ctx := context.Background()
	database, err := control.Open(ctx, t.TempDir()+"/control.sqlite")
	if err != nil {
		t.Fatalf("control.Open: %v", err)
	}
	t.Cleanup(func() {
		if closeErr := database.Close(); closeErr != nil {
			t.Errorf("close control database: %v", closeErr)
		}
	})
	if _, err := database.CreateIndex(ctx, adminTestIndex("main")); err != nil {
		t.Fatalf("CreateIndex(main): %v", err)
	}

	auditStore, err := audit.NewStore(database, audit.StoreOptions{
		CursorKey: []byte("audit-http-vertical-cursor-key-32"),
	})
	if err != nil {
		t.Fatalf("audit.NewStore: %v", err)
	}
	tokenStore, err := auth.NewStoreWithOptions(
		database,
		[]byte("audit-http-token-digest-key-0123456789"),
		auth.StoreOptions{
			AuditAppender:             auditStore,
			AuditTenantID:             browserGateTenantID,
			RequireExplicitAuditActor: true,
		},
	)
	if err != nil {
		t.Fatalf("auth.NewStoreWithOptions: %v", err)
	}
	browserAuthenticator, err := auth.NewBearerTokenAuthenticator(
		[]byte(adminIntegrationBearerToken),
		browserGateTenantID,
		browserGateOwnerID,
		auth.BrowserRoleAdministrator,
	)
	if err != nil {
		t.Fatalf("auth.NewBearerTokenAuthenticator: %v", err)
	}
	rawHandler := newTestHandler(t, Config{
		SearchJobs:                 &fakeSearchJobs{},
		Indexes:                    database,
		IngestionTokens:            tokenStore,
		AuditEvents:                auditStore,
		BrowserAuthenticator:       browserAuthenticator,
		WebUI:                      testUI(),
		TenantID:                   browserGateTenantID,
		OwnerID:                    browserGateOwnerID,
		AdministrativeAllowedHosts: []string{"example.com"},
	})
	handler := &adminIntegrationHandler{
		raw:   rawHandler,
		token: adminIntegrationBearerToken,
	}

	boundCollectorID := "collector-audit-vertical"
	createResponse := postProto(
		t,
		handler,
		"/api/ingestion-tokens/create",
		&opensplunk.CreateIngestionTokenRequest{
			Definition: &opensplunk.IngestionTokenDefinition{
				Name: "audited browser token",
				Constraints: &opensplunk.IngestionTokenConstraints{
					AllowedIndexNames: []string{"main"},
					BoundCollectorId:  &boundCollectorID,
				},
			},
		},
	)
	if createResponse.Code != http.StatusOK {
		t.Fatalf(
			"create token status = %d, body = %s",
			createResponse.Code,
			createResponse.Body.String(),
		)
	}
	var created opensplunk.CreateIngestionTokenResponse
	unmarshalResponse(t, createResponse, &created)
	plaintext := created.GetPlaintextToken()
	token := created.GetIngestionToken()
	if plaintext == "" || token.GetIngestionTokenId() == "" ||
		token.GetVersion() != 1 {
		t.Fatalf(
			"created ingestion token = %+v, plaintext length = %d",
			token,
			len(plaintext),
		)
	}

	auditResponse := postProto(
		t,
		handler,
		auditEventsListPath,
		&opensplunk.ListAuditEventsRequest{
			Page: &opensplunk.PageRequest{IncludeTotalSize: true},
		},
	)
	if auditResponse.Code != http.StatusOK {
		t.Fatalf(
			"list audit events status = %d, body = %s",
			auditResponse.Code,
			auditResponse.Body.String(),
		)
	}
	forbiddenSecrets := map[string]string{
		"plaintext token":      plaintext,
		"token prefix":         token.GetTokenPrefix(),
		"administrator bearer": adminIntegrationBearerToken,
	}
	for name, secret := range forbiddenSecrets {
		if secret != "" && bytes.Contains(auditResponse.Body.Bytes(), []byte(secret)) {
			t.Fatalf("audit response contains %s", name)
		}
	}

	var listed opensplunk.ListAuditEventsResponse
	unmarshalResponse(t, auditResponse, &listed)
	if len(listed.GetAuditEvents()) != 1 ||
		listed.GetPage().GetTotalSize() != 1 ||
		!listed.GetPage().GetTotalSizeExact() {
		t.Fatalf("audit list response = %+v", &listed)
	}
	event := listed.GetAuditEvents()[0]
	if event.GetSequence() != 1 ||
		event.GetActorKind() != opensplunk.AuditActorKind_AUDIT_ACTOR_KIND_BROWSER ||
		event.GetActorId() != browserGateOwnerID ||
		event.GetActorRole() != opensplunk.AuditActorRole_AUDIT_ACTOR_ROLE_ADMINISTRATOR ||
		event.GetAction() != opensplunk.AuditAction_AUDIT_ACTION_INGESTION_TOKEN_CREATE ||
		event.GetTargetKind() != opensplunk.AuditTargetKind_AUDIT_TARGET_KIND_INGESTION_TOKEN ||
		event.GetTargetId() != token.GetIngestionTokenId() ||
		event.GetTargetVersion() != token.GetVersion() {
		t.Fatalf("audited token creation = %+v", event)
	}

	var tenantID, actorKind, actorID, actorRole, action, targetKind, targetID string
	if err := database.SQLDB().QueryRowContext(ctx, `
		SELECT tenant_id, actor_kind, actor_id, actor_role, action, target_kind, target_id
		FROM audit_events
		WHERE sequence = 1`).Scan(
		&tenantID,
		&actorKind,
		&actorID,
		&actorRole,
		&action,
		&targetKind,
		&targetID,
	); err != nil {
		t.Fatalf("read persisted audit identity: %v", err)
	}
	if tenantID != browserGateTenantID || actorID != browserGateOwnerID ||
		actorKind != string(audit.ActorKindBrowser) ||
		actorRole != string(audit.ActorRoleAdministrator) ||
		action != string(audit.ActionIngestionTokenCreate) ||
		targetKind != string(audit.TargetKindIngestionToken) ||
		targetID != token.GetIngestionTokenId() {
		t.Fatalf(
			"persisted audit identity = tenant %q actor %q/%q/%q action %q target %q/%q",
			tenantID,
			actorKind,
			actorID,
			actorRole,
			action,
			targetKind,
			targetID,
		)
	}
	persistedText := strings.Join(
		[]string{tenantID, actorKind, actorID, actorRole, action, targetKind, targetID},
		"\x00",
	)
	for name, secret := range forbiddenSecrets {
		if secret != "" && strings.Contains(persistedText, secret) {
			t.Fatalf("persisted audit row contains %s", name)
		}
	}

	rows, err := database.SQLDB().QueryContext(
		ctx,
		`SELECT name FROM pragma_table_info('audit_events') ORDER BY cid`,
	)
	if err != nil {
		t.Fatalf("inspect audit event columns: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var column string
		if err := rows.Scan(&column); err != nil {
			t.Fatalf("scan audit event column: %v", err)
		}
		lower := strings.ToLower(column)
		for _, forbidden := range []string{
			"secret", "plaintext", "digest", "prefix", "authorization",
			"payload", "metadata",
		} {
			if strings.Contains(lower, forbidden) {
				t.Fatalf("audit schema contains forbidden column %q", column)
			}
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate audit event columns: %v", err)
	}
}

func TestAuditEventListRequiresAdministratorBeforeBodyOrServiceWork(t *testing.T) {
	t.Parallel()

	service := &fakeAuditEvents{}
	handler := newAuditTestHandler(t, service)
	body := &observedRequestBody{}
	request := httptest.NewRequestWithContext(
		context.Background(),
		http.MethodPost,
		auditEventsListPath,
		nil,
	)
	request.Body = body
	request.Host = "example.com"
	request.Header.Set("Content-Type", "text/plain")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusUnauthorized ||
		response.Header().Get("WWW-Authenticate") != administratorAuthenticationRealm {
		t.Fatalf("response = %d/%q/%q", response.Code, response.Header().Get("WWW-Authenticate"), response.Body.String())
	}
	calls, _, _ := service.snapshot()
	if body.reads != 0 || calls != 0 {
		t.Fatalf("unauthorized audit work = body reads %d, calls %d", body.reads, calls)
	}
}

func TestAuditEventListRejectsInvalidFiltersBeforeStorage(t *testing.T) {
	t.Parallel()

	target := opensplunk.AuditTargetKind_AUDIT_TARGET_KIND_UNSPECIFIED
	oversizedActor := string(bytes.Repeat([]byte{'a'}, 256))
	paddedActor := " administrator"
	tests := []struct {
		name    string
		request *opensplunk.ListAuditEventsRequest
	}{
		{
			name: "unspecified action",
			request: &opensplunk.ListAuditEventsRequest{ActionFilters: []opensplunk.AuditAction{
				opensplunk.AuditAction_AUDIT_ACTION_UNSPECIFIED,
			}},
		},
		{
			name: "duplicate action",
			request: &opensplunk.ListAuditEventsRequest{ActionFilters: []opensplunk.AuditAction{
				opensplunk.AuditAction_AUDIT_ACTION_INGESTION_TOKEN_CREATE,
				opensplunk.AuditAction_AUDIT_ACTION_INGESTION_TOKEN_CREATE,
			}},
		},
		{
			name:    "unspecified target",
			request: &opensplunk.ListAuditEventsRequest{TargetKindFilter: &target},
		},
		{
			name:    "oversized actor",
			request: &opensplunk.ListAuditEventsRequest{ActorIdFilter: &oversizedActor},
		},
		{
			name:    "padded actor",
			request: &opensplunk.ListAuditEventsRequest{ActorIdFilter: &paddedActor},
		},
		{
			name: "too many actions",
			request: &opensplunk.ListAuditEventsRequest{ActionFilters: []opensplunk.AuditAction{
				opensplunk.AuditAction_AUDIT_ACTION_INGESTION_TOKEN_CREATE,
				opensplunk.AuditAction_AUDIT_ACTION_INGESTION_TOKEN_UPDATE,
				opensplunk.AuditAction_AUDIT_ACTION_INGESTION_TOKEN_REVOKE,
				opensplunk.AuditAction_AUDIT_ACTION_INDEX_CREATE,
				opensplunk.AuditAction_AUDIT_ACTION_INDEX_UPDATE,
				opensplunk.AuditAction_AUDIT_ACTION_INDEX_ACTIVATE,
				opensplunk.AuditAction_AUDIT_ACTION_INDEX_ARCHIVE,
				opensplunk.AuditAction_AUDIT_ACTION_INDEX_DELETE_KEEP_DATA,
				opensplunk.AuditAction_AUDIT_ACTION_INDEX_DELETE_DATA,
				opensplunk.AuditAction_AUDIT_ACTION_APP_CREATE,
				opensplunk.AuditAction_AUDIT_ACTION_APP_UPDATE,
				opensplunk.AuditAction_AUDIT_ACTION_APP_ACTIVATE,
				opensplunk.AuditAction_AUDIT_ACTION_APP_ARCHIVE,
				opensplunk.AuditAction_AUDIT_ACTION_APP_DELETE,
				opensplunk.AuditAction_AUDIT_ACTION_APP_CREATE,
			}},
		},
		{
			name: "oversized page",
			request: &opensplunk.ListAuditEventsRequest{Page: &opensplunk.PageRequest{
				PageSize: new(uint32(audit.MaximumListPageSize + 1)),
			}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			service := &fakeAuditEvents{}
			handler := newAuditTestHandler(t, service)
			response := postAuthenticatedAudit(t, handler, test.request)
			if response.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
			}
			calls, _, _ := service.snapshot()
			if calls != 0 {
				t.Fatalf("invalid request reached audit storage %d times", calls)
			}
		})
	}
}

func TestAuditEventListClampsDefaultPageSizeToServerMaximum(t *testing.T) {
	t.Parallel()

	service := &fakeAuditEvents{}
	handler := newTestHandler(t, Config{
		SearchJobs:                 &fakeSearchJobs{},
		Indexes:                    fakeIndexCatalog{},
		AuditEvents:                service,
		BrowserAuthenticator:       testSearchInspectionAuthenticator(t),
		WebUI:                      testUI(),
		TenantID:                   browserGateTenantID,
		OwnerID:                    browserGateOwnerID,
		MaximumPageSize:            1,
		MaximumConcurrentResponses: 2,
		AdministrativeAllowedHosts: []string{"example.com"},
	})
	response := postAuthenticatedAudit(
		t,
		handler,
		&opensplunk.ListAuditEventsRequest{},
	)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	calls, _, request := service.snapshot()
	if calls != 1 || request.PageSize != 1 {
		t.Fatalf("audit list call = %d/%+v, want page size 1", calls, request)
	}
}

func TestAuditEventListMapsStorageFailureAndAdvertisesManagedFeature(t *testing.T) {
	t.Parallel()

	service := &fakeAuditEvents{err: audit.ErrCorrupt}
	handler := newAuditTestHandler(t, service)
	response := postAuthenticatedAudit(t, handler, &opensplunk.ListAuditEventsRequest{})
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("corrupt storage status = %d, body = %s", response.Code, response.Body.String())
	}

	bootstrap := postProto(
		t,
		handler,
		"/api/system/bootstrap",
		&opensplunk.GetSystemBootstrapRequest{},
	)
	if bootstrap.Code != http.StatusOK {
		t.Fatalf("bootstrap status = %d, body = %s", bootstrap.Code, bootstrap.Body.String())
	}
	var decoded opensplunk.GetSystemBootstrapResponse
	unmarshalResponse(t, bootstrap, &decoded)
	if !slices.Contains(decoded.GetFeatures(), opensplunk.ServerFeature_SERVER_FEATURE_AUDIT_SEARCH) {
		t.Fatalf("bootstrap features = %v, want audit search", decoded.GetFeatures())
	}

	disabled := newTestHandler(t, Config{
		SearchJobs:           &fakeSearchJobs{},
		Indexes:              fakeIndexCatalog{},
		BrowserAuthenticator: testSearchInspectionAuthenticator(t),
		WebUI:                testUI(),
		TenantID:             browserGateTenantID,
		OwnerID:              browserGateOwnerID,
		Bootstrap: BootstrapConfig{Features: []opensplunk.ServerFeature{
			opensplunk.ServerFeature_SERVER_FEATURE_AUDIT_SEARCH,
		}},
	})
	disabledBootstrap := postProto(t, disabled, "/api/system/bootstrap", &opensplunk.GetSystemBootstrapRequest{})
	unmarshalResponse(t, disabledBootstrap, &decoded)
	if slices.Contains(decoded.GetFeatures(), opensplunk.ServerFeature_SERVER_FEATURE_AUDIT_SEARCH) {
		t.Fatalf("unconfigured handler advertised audit search: %v", decoded.GetFeatures())
	}
}

func TestAuditEventListMapsInvalidCursorWithoutLeakingStorageDetails(t *testing.T) {
	t.Parallel()

	secret := "database-password-audit-secret"
	service := &fakeAuditEvents{err: errors.Join(audit.ErrInvalidCursor, errors.New(secret))}
	handler := newAuditTestHandler(t, service)
	response := postAuthenticatedAudit(t, handler, &opensplunk.ListAuditEventsRequest{})
	if response.Code != http.StatusBadRequest {
		t.Fatalf("invalid cursor status = %d, body = %s", response.Code, response.Body.String())
	}
	if bytes.Contains(response.Body.Bytes(), []byte(secret)) {
		t.Fatalf("audit API leaked storage error: %q", response.Body.String())
	}
}

func TestAuditEventListMapsConfigurationAndStorageFailuresToUnavailable(t *testing.T) {
	t.Parallel()

	secret := "audit-storage-dsn-secret"
	for _, operationErr := range []error{
		errors.New(secret),
		errors.Join(control.ErrInvalidArgument, errors.New(secret)),
	} {
		service := &fakeAuditEvents{err: operationErr}
		handler := newAuditTestHandler(t, service)
		response := postAuthenticatedAudit(
			t,
			handler,
			&opensplunk.ListAuditEventsRequest{},
		)
		if response.Code != http.StatusServiceUnavailable {
			t.Fatalf("storage failure status = %d, body = %s", response.Code, response.Body.String())
		}
		if bytes.Contains(response.Body.Bytes(), []byte(secret)) {
			t.Fatalf("audit API leaked storage error: %q", response.Body.String())
		}
	}
}

func newAuditTestHandler(t *testing.T, service AuditEvents) *Handler {
	t.Helper()
	return newTestHandler(t, Config{
		SearchJobs:                 &fakeSearchJobs{},
		Indexes:                    fakeIndexCatalog{},
		AuditEvents:                service,
		BrowserAuthenticator:       testSearchInspectionAuthenticator(t),
		WebUI:                      testUI(),
		TenantID:                   browserGateTenantID,
		OwnerID:                    browserGateOwnerID,
		MaximumConcurrentResponses: 2,
		AdministrativeAllowedHosts: []string{"example.com"},
	})
}

func postAuthenticatedAudit(
	t *testing.T,
	handler http.Handler,
	message proto.Message,
) *httptest.ResponseRecorder {
	t.Helper()
	payload, err := proto.Marshal(message)
	if err != nil {
		t.Fatalf("marshal audit request: %v", err)
	}
	request := httptest.NewRequestWithContext(
		context.Background(),
		http.MethodPost,
		auditEventsListPath,
		bytes.NewReader(payload),
	)
	request.Host = "example.com"
	request.Header.Set("Content-Type", "application/x-protobuf")
	request.Header.Set("Authorization", "Bearer "+adminIntegrationBearerToken)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

func containsAny(value string, candidates []string) bool {
	return slices.ContainsFunc(candidates, func(candidate string) bool {
		return bytes.Contains([]byte(value), []byte(candidate))
	})
}

func TestAuditEventProjectionRejectsInvalidServiceRows(t *testing.T) {
	t.Parallel()

	anchor := time.Date(2026, time.August, 3, 20, 21, 22, 0, time.UTC)
	valid := audit.Event{
		Sequence:   1,
		TenantID:   browserGateTenantID,
		OccurredAt: anchor,
		Actor: audit.Actor{
			Kind: audit.ActorKindSystem,
			ID:   "open-splunk-server",
			Role: audit.ActorRoleSystem,
		},
		Action:        audit.ActionIngestionTokenCreate,
		TargetKind:    audit.TargetKindIngestionToken,
		TargetID:      "token-1",
		TargetVersion: 1,
	}
	for _, mutate := range []func(*audit.Event){
		func(event *audit.Event) { event.Sequence = 0 },
		func(event *audit.Event) { event.TenantID = "other-tenant" },
		func(event *audit.Event) { event.OccurredAt = time.Time{} },
		func(event *audit.Event) { event.OccurredAt = time.UnixMicro(0).UTC() },
		func(event *audit.Event) { event.OccurredAt = anchor.Add(time.Nanosecond) },
		func(event *audit.Event) {
			event.OccurredAt = anchor.In(time.FixedZone("fixture", 60*60))
		},
		func(event *audit.Event) { event.Actor = audit.Actor{} },
		func(event *audit.Event) {
			event.Actor = audit.Actor{
				Kind: audit.ActorKindBrowser,
				ID:   "ordinary-user",
				Role: audit.ActorRoleUser,
			}
		},
		func(event *audit.Event) { event.Action = "forged" },
		func(event *audit.Event) { event.TargetKind = "forged" },
		func(event *audit.Event) { event.TargetID = " padded-target" },
		func(event *audit.Event) { event.TargetVersion = 0 },
		func(event *audit.Event) { event.TargetVersion = 2 },
		func(event *audit.Event) {
			event.Action = audit.ActionIngestionTokenUpdate
			event.TargetVersion = 1
		},
		func(event *audit.Event) { event.TargetVersion = uint64(math.MaxInt64) + 1 },
	} {
		event := valid
		mutate(&event)
		service := &fakeAuditEvents{page: audit.ListPage{Events: []audit.Event{event}}}
		handler := newAuditTestHandler(t, service)
		response := postAuthenticatedAudit(t, handler, &opensplunk.ListAuditEventsRequest{})
		if response.Code != http.StatusInternalServerError {
			t.Fatalf("invalid event %#v status = %d, body = %s", event, response.Code, response.Body.String())
		}
	}
}

func TestAuditEventProjectionRejectsRowsOutsideRequestedFiltersAndTotals(t *testing.T) {
	t.Parallel()

	actorID := "administrator-a"
	targetKind := audit.TargetKindIngestionToken
	request := audit.ListRequest{
		PageSize:      1,
		ActionFilters: []audit.Action{audit.ActionIngestionTokenUpdate},
		ActorID:       &actorID,
		TargetKind:    &targetKind,
		IncludeTotal:  true,
	}
	valid := audit.Event{
		Sequence:   2,
		TenantID:   browserGateTenantID,
		OccurredAt: time.Date(2026, time.August, 3, 20, 21, 22, 0, time.UTC),
		Actor: audit.Actor{
			Kind: audit.ActorKindBrowser,
			ID:   actorID,
			Role: audit.ActorRoleAdministrator,
		},
		Action:        audit.ActionIngestionTokenUpdate,
		TargetKind:    audit.TargetKindIngestionToken,
		TargetID:      "token-1",
		TargetVersion: 2,
	}
	total := uint64(1)
	for name, mutate := range map[string]func(*audit.Event){
		"action": func(event *audit.Event) {
			event.Action = audit.ActionIngestionTokenRevoke
		},
		"actor": func(event *audit.Event) {
			event.Actor.ID = "administrator-b"
		},
		"target": func(event *audit.Event) {
			event.TargetKind = audit.TargetKind("other")
		},
	} {
		t.Run(name, func(t *testing.T) {
			event := valid
			mutate(&event)
			if response, err := auditListPageToProto(
				browserGateTenantID,
				request,
				audit.ListPage{Events: []audit.Event{event}, TotalSize: &total, TotalSizeExact: true},
			); response != nil || err == nil {
				t.Fatalf("auditListPageToProto(%s) = (%v, %v)", name, response, err)
			}
		})
	}

	tooLarge := uint64(audit.MaximumEventsPerTenant + 1)
	if response, err := auditListPageToProto(
		browserGateTenantID,
		request,
		audit.ListPage{Events: []audit.Event{valid}, TotalSize: &tooLarge, TotalSizeExact: true},
	); response != nil || err == nil {
		t.Fatalf("auditListPageToProto(large total) = (%v, %v)", response, err)
	}
}

func TestAuditProtoProjectionIsDeterministic(t *testing.T) {
	t.Parallel()

	event := audit.Event{
		Sequence:   1,
		TenantID:   browserGateTenantID,
		OccurredAt: time.Date(2026, time.August, 3, 20, 21, 22, 0, time.UTC),
		Actor: audit.Actor{
			Kind: audit.ActorKindBrowser,
			ID:   browserGateOwnerID,
			Role: audit.ActorRoleAdministrator,
		},
		Action:        audit.ActionIngestionTokenUpdate,
		TargetKind:    audit.TargetKindIngestionToken,
		TargetID:      "token-1",
		TargetVersion: 2,
	}
	first, err := auditEventToProto(event, browserGateTenantID)
	if err != nil {
		t.Fatalf("auditEventToProto(first): %v", err)
	}
	second, err := auditEventToProto(event, browserGateTenantID)
	if err != nil {
		t.Fatalf("auditEventToProto(second): %v", err)
	}
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("audit projection is not deterministic: %#v/%#v", first, second)
	}
}
