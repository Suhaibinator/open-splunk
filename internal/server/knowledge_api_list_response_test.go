package server

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/auth"
	"github.com/Suhaibinator/open-splunk/internal/knowledgeattemptaudit"
	"github.com/Suhaibinator/open-splunk/internal/knowledgecatalog"
	"github.com/Suhaibinator/open-splunk/internal/knowledgedefinition"
	"google.golang.org/protobuf/proto"
)

func TestKnowledgeHTTPListUsesDetachedCanonicalCatalogRequest(t *testing.T) {
	t.Parallel()

	object := knowledgeListResponseObject(
		t,
		"ko-list-canonical",
		"canonical-needle",
		knowledgecatalog.ObjectTypeFieldAlias,
		knowledgecatalog.SharingScopePrivate,
		knowledgeHTTPAppID,
		knowledgeBoundaryOwnerID,
	)
	description := "canonical needle description"
	object.Definition.Description = &description
	object.Definition.Selector = &opensplunk.KnowledgeSelector{
		HostPatterns: []*opensplunk.KnowledgeSelectorPattern{{Value: "prod-east-*"}},
	}
	object = canonicalKnowledgeListResponseObject(t, object)
	total := uint64(1)

	catalog := &knowledgeHTTPCatalog{listFn: func(
		_ context.Context,
		_ knowledgecatalog.ReadScope,
		request knowledgecatalog.ListRequest,
	) (knowledgecatalog.ListPage, error) {
		if request.PageSize != knowledgecatalog.DefaultPageSize ||
			!request.IncludeTotal || request.PageToken != "" ||
			request.AppIDFilter == nil || *request.AppIDFilter != knowledgeHTTPAppID ||
			request.OwnerIDFilter == nil || *request.OwnerIDFilter != knowledgeBoundaryOwnerID ||
			request.TextFilter == nil || *request.TextFilter != "needle" ||
			request.SelectorTextFilter == nil || *request.SelectorTextFilter != "prod" ||
			len(request.ObjectTypeFilters) != 1 ||
			request.ObjectTypeFilters[0] != knowledgecatalog.ObjectTypeFieldAlias ||
			len(request.StateFilters) != 1 || request.StateFilters[0] != knowledgecatalog.StateDraft ||
			len(request.SharingScopeFilters) != 1 ||
			request.SharingScopeFilters[0] != knowledgecatalog.SharingScopePrivate ||
			request.SortBy != knowledgecatalog.SortByName ||
			request.SortDirection != knowledgecatalog.SortAscending {
			t.Fatalf("catalog request is not canonical: %#v", request)
		}

		// The handler retains a separate canonical authority snapshot for response
		// validation; mutating the service-owned request must not affect it.
		*request.AppIDFilter = knowledgeHTTPOtherAppID
		*request.OwnerIDFilter = "attacker-owner"
		*request.TextFilter = "absent"
		*request.SelectorTextFilter = "absent"
		request.ObjectTypeFilters[0] = knowledgecatalog.ObjectTypeFieldExtraction
		request.StateFilters[0] = knowledgecatalog.StateActive
		request.SharingScopeFilters[0] = knowledgecatalog.SharingScopeGlobal
		request.SortDirection = knowledgecatalog.SortDescending

		return knowledgecatalog.ListPage{
			Objects:         []knowledgecatalog.Object{object},
			TotalSize:       &total,
			TotalSizeExact:  true,
			CatalogRevision: 1,
		}, nil
	}}
	appender := &knowledgeBoundaryAppender{}
	_, handler := newKnowledgeHTTPHandler(
		t,
		auth.BrowserRoleAdministrator,
		catalog,
		&knowledgeHTTPWriter{},
		knowledgeHTTPApps(),
		appender,
	)
	response := knowledgeHTTPPost(t, handler, knowledgeObjectsListPath, &opensplunk.ListKnowledgeObjectsRequest{
		Page:               &opensplunk.PageRequest{IncludeTotalSize: true},
		AppIdFilter:        new(" \t" + knowledgeHTTPAppID + "\r"),
		OwnerIdFilter:      new("\n" + knowledgeBoundaryOwnerID + " "),
		TextFilter:         new(" needle "),
		SelectorTextFilter: new("\tprod\r"),
		ObjectTypeFilters: []opensplunk.KnowledgeObjectType{
			opensplunk.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_FIELD_ALIAS,
			opensplunk.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_FIELD_ALIAS,
		},
		StateFilters: []opensplunk.KnowledgeObjectState{
			opensplunk.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DRAFT,
			opensplunk.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DRAFT,
		},
		SharingScopeFilters: []opensplunk.SharingScope{
			opensplunk.SharingScope_SHARING_SCOPE_PRIVATE,
			opensplunk.SharingScope_SHARING_SCOPE_PRIVATE,
		},
	})
	if response.Code != http.StatusOK || len(appender.snapshot()) != 0 {
		t.Fatalf("status=%d body=%q attempts=%+v", response.Code, response.Body.String(), appender.snapshot())
	}
}

func TestKnowledgeHTTPListRejectsAmplifyingCardinalityBeforeSerialization(
	t *testing.T,
) {
	t.Parallel()

	request := &opensplunk.ListKnowledgeObjectsRequest{
		ObjectTypeFilters: make([]opensplunk.KnowledgeObjectType, 8<<10),
	}
	for index := range request.ObjectTypeFilters {
		request.ObjectTypeFilters[index] =
			opensplunk.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_FIELD_ALIAS
	}
	if size := proto.Size(request); size >= int(maximumKnowledgeSmallRequestBytes) {
		t.Fatalf("amplifying request size=%d, transport maximum=%d", size, maximumKnowledgeSmallRequestBytes)
	}
	catalog := &knowledgeHTTPCatalog{}
	apps := knowledgeHTTPApps()
	appender := &knowledgeBoundaryAppender{}
	handler, httpHandler := newKnowledgeHTTPHandler(
		t,
		auth.BrowserRoleAdministrator,
		catalog,
		&knowledgeHTTPWriter{},
		apps,
		appender,
	)
	for range cap(handler.serializationGate) {
		handler.serializationGate <- struct{}{}
	}
	response := knowledgeHTTPPost(t, httpHandler, knowledgeObjectsListPath, request)
	for range cap(handler.serializationGate) {
		<-handler.serializationGate
	}
	_, listCalls := catalog.calls()
	attempts := appender.snapshot()
	if response.Code != http.StatusBadRequest || apps.callCount() != 0 ||
		listCalls != 0 || len(attempts) != 1 ||
		attempts[0].definition.Action != knowledgeattemptaudit.ActionList ||
		attempts[0].definition.Reason != knowledgeattemptaudit.ReasonInvalidDefinition {
		t.Fatalf(
			"status=%d body=%q apps=%d catalog=%d attempts=%+v",
			response.Code,
			response.Body.String(),
			apps.callCount(),
			listCalls,
			attempts,
		)
	}
}

func TestKnowledgeHTTPListPreflightUsesNormalizedOptionalFilterByteLimits(t *testing.T) {
	t.Parallel()

	maximumApp := strings.Repeat("a", maximumKnowledgeAppIDBytes)
	maximumIdentity := strings.Repeat("i", maximumKnowledgeIdentityBytes)
	maximumText := strings.Repeat("t", maximumKnowledgeIdentityBytes)
	maximumSelector := strings.Repeat("s", maximumKnowledgeIdentityBytes)
	pad := func(value string) *string {
		return new(" \t\n" + value + "\r ")
	}
	catalog := &knowledgeHTTPCatalog{listFn: func(
		_ context.Context,
		_ knowledgecatalog.ReadScope,
		request knowledgecatalog.ListRequest,
	) (knowledgecatalog.ListPage, error) {
		if request.AppIDFilter == nil || *request.AppIDFilter != maximumApp ||
			request.OwnerIDFilter == nil || *request.OwnerIDFilter != maximumIdentity ||
			request.TextFilter == nil || *request.TextFilter != maximumText ||
			request.SelectorTextFilter == nil || *request.SelectorTextFilter != maximumSelector {
			t.Fatalf("normalized catalog filters = %#v", request)
		}
		return knowledgecatalog.ListPage{}, nil
	}}
	appender := &knowledgeBoundaryAppender{}
	_, handler := newKnowledgeHTTPHandler(
		t,
		auth.BrowserRoleAdministrator,
		catalog,
		&knowledgeHTTPWriter{},
		knowledgeHTTPApps(),
		appender,
	)
	response := knowledgeHTTPPost(t, handler, knowledgeObjectsListPath, &opensplunk.ListKnowledgeObjectsRequest{
		AppIdFilter:        pad(maximumApp),
		OwnerIdFilter:      pad(maximumIdentity),
		TextFilter:         pad(maximumText),
		SelectorTextFilter: pad(maximumSelector),
	})
	_, listCalls := catalog.calls()
	if response.Code != http.StatusOK || listCalls != 1 || len(appender.snapshot()) != 0 {
		t.Fatalf(
			"status=%d body=%q calls=%d attempts=%+v",
			response.Code,
			response.Body.String(),
			listCalls,
			appender.snapshot(),
		)
	}
}

func TestKnowledgeHTTPListPreflightRejectsNormalizedOptionalFilterAboveLimit(
	t *testing.T,
) {
	t.Parallel()

	tests := []struct {
		name    string
		request *opensplunk.ListKnowledgeObjectsRequest
	}{
		{
			name:    "app ID",
			request: &opensplunk.ListKnowledgeObjectsRequest{AppIdFilter: new(" \t" + strings.Repeat("a", maximumKnowledgeAppIDBytes+1) + "\r ")},
		},
		{
			name:    "owner ID",
			request: &opensplunk.ListKnowledgeObjectsRequest{OwnerIdFilter: new(" \t" + strings.Repeat("o", maximumKnowledgeIdentityBytes+1) + "\r ")},
		},
		{
			name:    "text",
			request: &opensplunk.ListKnowledgeObjectsRequest{TextFilter: new(" \t" + strings.Repeat("t", maximumKnowledgeIdentityBytes+1) + "\r ")},
		},
		{
			name:    "selector text",
			request: &opensplunk.ListKnowledgeObjectsRequest{SelectorTextFilter: new(" \t" + strings.Repeat("s", maximumKnowledgeIdentityBytes+1) + "\r ")},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			catalog := &knowledgeHTTPCatalog{listFn: func(
				context.Context,
				knowledgecatalog.ReadScope,
				knowledgecatalog.ListRequest,
			) (knowledgecatalog.ListPage, error) {
				return knowledgecatalog.ListPage{}, nil
			}}
			apps := knowledgeHTTPApps()
			appender := &knowledgeBoundaryAppender{}
			api, handler := newKnowledgeHTTPHandler(
				t,
				auth.BrowserRoleAdministrator,
				catalog,
				&knowledgeHTTPWriter{},
				apps,
				appender,
			)
			for index := 0; index < cap(api.serializationGate); index++ {
				api.serializationGate <- struct{}{}
			}
			response := knowledgeHTTPPost(t, handler, knowledgeObjectsListPath, test.request)
			for index := 0; index < cap(api.serializationGate); index++ {
				<-api.serializationGate
			}
			_, listCalls := catalog.calls()
			attempts := appender.snapshot()
			if response.Code != http.StatusBadRequest || apps.callCount() != 0 ||
				listCalls != 0 ||
				len(attempts) != 1 ||
				attempts[0].definition.Action != knowledgeattemptaudit.ActionList ||
				attempts[0].definition.Reason != knowledgeattemptaudit.ReasonInvalidDefinition {
				t.Fatalf(
					"status=%d body=%q apps=%d calls=%d attempts=%+v",
					response.Code,
					response.Body.String(),
					apps.callCount(),
					listCalls,
					attempts,
				)
			}
		})
	}
}

func TestKnowledgeHTTPListRejectsObjectsOutsideEveryNormalizedFilter(t *testing.T) {
	t.Parallel()

	private := knowledgeListResponseObject(
		t,
		"ko-list-filter",
		"filter-object",
		knowledgecatalog.ObjectTypeFieldAlias,
		knowledgecatalog.SharingScopePrivate,
		knowledgeHTTPAppID,
		knowledgeBoundaryOwnerID,
	)
	tests := []struct {
		name    string
		request *opensplunk.ListKnowledgeObjectsRequest
		object  knowledgecatalog.Object
	}{
		{
			name: "app ID",
			request: &opensplunk.ListKnowledgeObjectsRequest{
				AppIdFilter: new(knowledgeHTTPAppID),
			},
			object: knowledgeListResponseObject(
				t,
				"ko-list-foreign-app",
				"filter-object",
				knowledgecatalog.ObjectTypeFieldAlias,
				knowledgecatalog.SharingScopeGlobal,
				knowledgeHTTPOtherAppID,
				knowledgeBoundaryOwnerID,
			),
		},
		{
			name: "owner ID",
			request: &opensplunk.ListKnowledgeObjectsRequest{
				OwnerIdFilter: new(knowledgeBoundaryOwnerID),
			},
			object: knowledgeListResponseObject(
				t,
				"ko-list-foreign-owner",
				"filter-object",
				knowledgecatalog.ObjectTypeFieldAlias,
				knowledgecatalog.SharingScopeGlobal,
				knowledgeHTTPAppID,
				"foreign-owner",
			),
		},
		{
			name:    "name or description text",
			request: &opensplunk.ListKnowledgeObjectsRequest{TextFilter: new("absent")},
			object:  private,
		},
		{
			name:    "selector text",
			request: &opensplunk.ListKnowledgeObjectsRequest{SelectorTextFilter: new("absent")},
			object:  private,
		},
		{
			name: "object type",
			request: &opensplunk.ListKnowledgeObjectsRequest{ObjectTypeFilters: []opensplunk.KnowledgeObjectType{
				opensplunk.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_FIELD_EXTRACTION,
			}},
			object: private,
		},
		{
			name: "state",
			request: &opensplunk.ListKnowledgeObjectsRequest{StateFilters: []opensplunk.KnowledgeObjectState{
				opensplunk.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_ACTIVE,
			}},
			object: private,
		},
		{
			name: "sharing scope",
			request: &opensplunk.ListKnowledgeObjectsRequest{SharingScopeFilters: []opensplunk.SharingScope{
				opensplunk.SharingScope_SHARING_SCOPE_GLOBAL,
			}},
			object: private,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			assertKnowledgeHTTPListPageRejected(t, test.request, knowledgecatalog.ListPage{
				Objects:         []knowledgecatalog.Object{test.object},
				CatalogRevision: 2,
			})
		})
	}
}

func TestKnowledgeHTTPListRejectsDuplicatesAndNonCanonicalOrdering(t *testing.T) {
	t.Parallel()

	created := time.Date(2026, 8, 7, 10, 0, 0, 0, time.UTC)
	object := func(id, name string) knowledgecatalog.Object {
		result := knowledgeListResponseObject(
			t,
			id,
			name,
			knowledgecatalog.ObjectTypeFieldAlias,
			knowledgecatalog.SharingScopePrivate,
			knowledgeHTTPAppID,
			knowledgeBoundaryOwnerID,
		)
		result.CreatedAt = created
		result.UpdatedAt = created
		return result
	}
	alpha := object("ko-list-alpha", "alpha")
	bravo := object("ko-list-bravo", "bravo")
	laterCreated := alpha
	laterCreated.KnowledgeObjectID = "ko-list-created-later"
	laterCreated.CreatedAt = created.Add(time.Microsecond)
	laterCreated.UpdatedAt = laterCreated.CreatedAt
	laterUpdated := alpha
	laterUpdated.KnowledgeObjectID = "ko-list-updated-later"
	laterUpdated.UpdatedAt = created.Add(time.Microsecond)
	extraction := knowledgeListResponseObject(
		t,
		"ko-list-extraction",
		"alpha",
		knowledgecatalog.ObjectTypeFieldExtraction,
		knowledgecatalog.SharingScopePrivate,
		knowledgeHTTPAppID,
		knowledgeBoundaryOwnerID,
	)
	tieZulu := object("ko-list-zulu", "same-name")
	tieAlpha := object("ko-list-alpha-tie", "same-name")
	duplicateA := object("ko-list-duplicate", "alpha")
	duplicateB := object("ko-list-duplicate", "bravo")

	tests := []struct {
		name      string
		sortBy    opensplunk.KnowledgeObjectSortBy
		direction opensplunk.SortDirection
		objects   []knowledgecatalog.Object
	}{
		{name: "duplicate object ID", objects: []knowledgecatalog.Object{duplicateA, duplicateB}},
		{name: "name ascending", objects: []knowledgecatalog.Object{bravo, alpha}},
		{name: "name descending", direction: opensplunk.SortDirection_SORT_DIRECTION_DESCENDING, objects: []knowledgecatalog.Object{alpha, bravo}},
		{name: "name object-ID tie break", objects: []knowledgecatalog.Object{tieZulu, tieAlpha}},
		{name: "created at", sortBy: opensplunk.KnowledgeObjectSortBy_KNOWLEDGE_OBJECT_SORT_BY_CREATED_AT, objects: []knowledgecatalog.Object{laterCreated, alpha}},
		{name: "updated at", sortBy: opensplunk.KnowledgeObjectSortBy_KNOWLEDGE_OBJECT_SORT_BY_UPDATED_AT, objects: []knowledgecatalog.Object{laterUpdated, alpha}},
		{name: "object type", sortBy: opensplunk.KnowledgeObjectSortBy_KNOWLEDGE_OBJECT_SORT_BY_OBJECT_TYPE, objects: []knowledgecatalog.Object{extraction, alpha}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			assertKnowledgeHTTPListPageRejected(t, &opensplunk.ListKnowledgeObjectsRequest{
				Page:          &opensplunk.PageRequest{PageSize: new(uint32(2))},
				SortBy:        test.sortBy,
				SortDirection: test.direction,
			}, knowledgecatalog.ListPage{Objects: test.objects, CatalogRevision: 2})
		})
	}
}

func TestKnowledgeHTTPListRejectsIncoherentContinuationMetadata(t *testing.T) {
	t.Parallel()

	object := knowledgeListResponseObject(
		t,
		"ko-list-page",
		"page-object",
		knowledgecatalog.ObjectTypeFieldAlias,
		knowledgecatalog.SharingScopePrivate,
		knowledgeHTTPAppID,
		knowledgeBoundaryOwnerID,
	)
	one := uint64(1)
	two := uint64(2)
	overCatalog := uint64(knowledgecatalog.MaximumObjectsPerTenant + 1)
	tests := []struct {
		name    string
		request *opensplunk.ListKnowledgeObjectsRequest
		page    knowledgecatalog.ListPage
	}{
		{name: "oversized next token", request: &opensplunk.ListKnowledgeObjectsRequest{}, page: knowledgecatalog.ListPage{Objects: []knowledgecatalog.Object{object}, NextPageToken: strings.Repeat("x", maximumKnowledgePageTokenBytes+1), CatalogRevision: 2}},
		{name: "control next token", request: &opensplunk.ListKnowledgeObjectsRequest{}, page: knowledgecatalog.ListPage{Objects: []knowledgecatalog.Object{object}, NextPageToken: "next\x00token", CatalogRevision: 2}},
		{name: "control request token on successful page", request: &opensplunk.ListKnowledgeObjectsRequest{Page: &opensplunk.PageRequest{PageToken: new("current\x01token")}}, page: knowledgecatalog.ListPage{Objects: []knowledgecatalog.Object{object}, CatalogRevision: 2}},
		{name: "echoed continuation", request: &opensplunk.ListKnowledgeObjectsRequest{Page: &opensplunk.PageRequest{PageToken: new("same-token")}}, page: knowledgecatalog.ListPage{Objects: []knowledgecatalog.Object{object}, NextPageToken: "same-token", CatalogRevision: 2}},
		{name: "empty continued page", request: &opensplunk.ListKnowledgeObjectsRequest{Page: &opensplunk.PageRequest{PageToken: new("current-token")}}, page: knowledgecatalog.ListPage{CatalogRevision: 2}},
		{name: "continuation without revision", request: &opensplunk.ListKnowledgeObjectsRequest{Page: &opensplunk.PageRequest{PageToken: new("current-token")}}, page: knowledgecatalog.ListPage{Objects: []knowledgecatalog.Object{object}}},
		{name: "unexpected total", request: &opensplunk.ListKnowledgeObjectsRequest{}, page: knowledgecatalog.ListPage{Objects: []knowledgecatalog.Object{object}, TotalSize: &one, TotalSizeExact: true, CatalogRevision: 2}},
		{name: "missing total", request: &opensplunk.ListKnowledgeObjectsRequest{Page: &opensplunk.PageRequest{IncludeTotalSize: true}}, page: knowledgecatalog.ListPage{Objects: []knowledgecatalog.Object{object}, CatalogRevision: 2}},
		{name: "inexact total", request: &opensplunk.ListKnowledgeObjectsRequest{Page: &opensplunk.PageRequest{IncludeTotalSize: true}}, page: knowledgecatalog.ListPage{Objects: []knowledgecatalog.Object{object}, TotalSize: &one, CatalogRevision: 2}},
		{name: "terminal first-page total mismatch", request: &opensplunk.ListKnowledgeObjectsRequest{Page: &opensplunk.PageRequest{IncludeTotalSize: true}}, page: knowledgecatalog.ListPage{Objects: []knowledgecatalog.Object{object}, TotalSize: &two, TotalSizeExact: true, CatalogRevision: 2}},
		{name: "total exceeds catalog identity ceiling", request: &opensplunk.ListKnowledgeObjectsRequest{Page: &opensplunk.PageRequest{IncludeTotalSize: true}}, page: knowledgecatalog.ListPage{Objects: []knowledgecatalog.Object{object}, NextPageToken: "next-token", TotalSize: &overCatalog, TotalSizeExact: true, CatalogRevision: 2}},
		{name: "continued total omits earlier rows", request: &opensplunk.ListKnowledgeObjectsRequest{Page: &opensplunk.PageRequest{PageToken: new("current-token"), IncludeTotalSize: true}}, page: knowledgecatalog.ListPage{Objects: []knowledgecatalog.Object{object}, TotalSize: &one, TotalSizeExact: true, CatalogRevision: 2}},
		{name: "next token without remaining total", request: &opensplunk.ListKnowledgeObjectsRequest{Page: &opensplunk.PageRequest{IncludeTotalSize: true}}, page: knowledgecatalog.ListPage{Objects: []knowledgecatalog.Object{object}, NextPageToken: "next-token", TotalSize: &one, TotalSizeExact: true, CatalogRevision: 2}},
		{name: "both-side continuation omits one side", request: &opensplunk.ListKnowledgeObjectsRequest{Page: &opensplunk.PageRequest{PageToken: new("current-token"), IncludeTotalSize: true}}, page: knowledgecatalog.ListPage{Objects: []knowledgecatalog.Object{object}, NextPageToken: "next-token", TotalSize: &two, TotalSizeExact: true, CatalogRevision: 2}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			assertKnowledgeHTTPListPageRejected(t, test.request, test.page)
		})
	}
}

func TestKnowledgeListPageAllowsBudgetShortContinuationAndCoherentFinalPage(t *testing.T) {
	t.Parallel()

	object := knowledgeListResponseObject(
		t,
		"ko-list-short-page",
		"short-page",
		knowledgecatalog.ObjectTypeFieldAlias,
		knowledgecatalog.SharingScopePrivate,
		knowledgeHTTPAppID,
		knowledgeBoundaryOwnerID,
	)
	scopes := knowledgeListResponseScopes()
	first, err := knowledgeListPageToProto(scopes, knowledgecatalog.ListRequest{
		PageSize:      2,
		SortBy:        knowledgecatalog.SortByName,
		SortDirection: knowledgecatalog.SortAscending,
	}, knowledgecatalog.ListPage{
		Objects:         []knowledgecatalog.Object{object},
		NextPageToken:   "next-token",
		CatalogRevision: 2,
	})
	if err != nil || first.GetPage().GetNextPageToken() != "next-token" {
		t.Fatalf("budget-short first page = %#v, %v", first, err)
	}
	full, err := knowledgeListPageToProto(scopes, knowledgecatalog.ListRequest{
		PageSize:      1,
		SortBy:        knowledgecatalog.SortByName,
		SortDirection: knowledgecatalog.SortAscending,
	}, knowledgecatalog.ListPage{
		Objects:         []knowledgecatalog.Object{object},
		NextPageToken:   "full-next-token",
		CatalogRevision: 2,
	})
	if err != nil || full.GetPage().GetNextPageToken() != "full-next-token" {
		t.Fatalf("full continued page = %#v, %v", full, err)
	}
	total := uint64(2)
	final, err := knowledgeListPageToProto(scopes, knowledgecatalog.ListRequest{
		PageSize:      2,
		PageToken:     "next-token",
		IncludeTotal:  true,
		SortBy:        knowledgecatalog.SortByName,
		SortDirection: knowledgecatalog.SortAscending,
	}, knowledgecatalog.ListPage{
		Objects:         []knowledgecatalog.Object{object},
		TotalSize:       &total,
		TotalSizeExact:  true,
		CatalogRevision: 2,
	})
	if err != nil || final.GetPage().GetTotalSize() != 2 {
		t.Fatalf("coherent final page = %#v, %v", final, err)
	}
	bothSideTotal := uint64(3)
	bothSide, err := knowledgeListPageToProto(scopes, knowledgecatalog.ListRequest{
		PageSize:      2,
		PageToken:     "current-token",
		IncludeTotal:  true,
		SortBy:        knowledgecatalog.SortByName,
		SortDirection: knowledgecatalog.SortAscending,
	}, knowledgecatalog.ListPage{
		Objects:         []knowledgecatalog.Object{object},
		NextPageToken:   "next-token",
		TotalSize:       &bothSideTotal,
		TotalSizeExact:  true,
		CatalogRevision: 2,
	})
	if err != nil || bothSide.GetPage().GetTotalSize() != 3 ||
		bothSide.GetPage().GetNextPageToken() != "next-token" {
		t.Fatalf("coherent both-side page = %#v, %v", bothSide, err)
	}
}

func TestKnowledgeListPageAllowsTenantGlobalProvenanceOutsideReadableApps(t *testing.T) {
	t.Parallel()

	object := knowledgeListResponseObject(
		t,
		"ko-list-global-provenance",
		"global-provenance",
		knowledgecatalog.ObjectTypeFieldAlias,
		knowledgecatalog.SharingScopeGlobal,
		knowledgeHTTPOtherAppID,
		"foreign-owner",
	)
	response, err := knowledgeListPageToProto(
		knowledgeListResponseScopes(),
		knowledgecatalog.ListRequest{
			PageSize:      1,
			SortBy:        knowledgecatalog.SortByName,
			SortDirection: knowledgecatalog.SortAscending,
		},
		knowledgecatalog.ListPage{
			Objects:         []knowledgecatalog.Object{object},
			CatalogRevision: 2,
		},
	)
	if err != nil || len(response.GetKnowledgeObjects()) != 1 ||
		response.GetKnowledgeObjects()[0].GetAppId() != knowledgeHTTPOtherAppID {
		t.Fatalf("global provenance page = %#v, %v", response, err)
	}
}

func TestKnowledgeListPagePreflightsDefinitionAndResponseAllocationBounds(t *testing.T) {
	base := knowledgeListResponseObject(
		t,
		"ko-list-allocation-a",
		"allocation-a",
		knowledgecatalog.ObjectTypeFieldAlias,
		knowledgecatalog.SharingScopePrivate,
		knowledgeHTTPAppID,
		knowledgeBoundaryOwnerID,
	)
	request := knowledgecatalog.ListRequest{
		PageSize:      2,
		SortBy:        knowledgecatalog.SortByName,
		SortDirection: knowledgecatalog.SortAscending,
	}
	t.Run("per definition", func(t *testing.T) {
		object := base
		definition := proto.Clone(base.Definition).(*opensplunk.KnowledgeObjectDefinition)
		description := strings.Repeat("x", knowledgecatalog.MaximumListResponseCanonicalDefinitionBytes+1)
		definition.Description = &description
		object.Definition = definition
		if _, err := knowledgeListPageToProto(
			knowledgeListResponseScopes(),
			request,
			knowledgecatalog.ListPage{Objects: []knowledgecatalog.Object{object}, CatalogRevision: 2},
		); err == nil {
			t.Fatal("oversized definition page succeeded")
		}
	})
	t.Run("aggregate definitions", func(t *testing.T) {
		first := base
		second := base
		second.KnowledgeObjectID = "ko-list-allocation-b"
		second.Name = "allocation-b"
		for index, object := range []*knowledgecatalog.Object{&first, &second} {
			definition := proto.Clone(base.Definition).(*opensplunk.KnowledgeObjectDefinition)
			definition.Name = object.Name
			description := strings.Repeat(
				string(rune('a'+index)),
				knowledgecatalog.MaximumListResponseCanonicalDefinitionBytes/2+1,
			)
			definition.Description = &description
			object.Definition = definition
		}
		if _, err := knowledgeListPageToProto(
			knowledgeListResponseScopes(),
			request,
			knowledgecatalog.ListPage{Objects: []knowledgecatalog.Object{first, second}, CatalogRevision: 2},
		); err == nil {
			t.Fatal("aggregate oversized definition page succeeded")
		}
	})
	t.Run("pre-response envelope", func(t *testing.T) {
		object := base
		reason := strings.Repeat("x", maximumKnowledgeListResponseBytes+1)
		object.QuarantineReason = &reason
		if _, err := knowledgeListPageToProto(
			knowledgeListResponseScopes(),
			request,
			knowledgecatalog.ListPage{Objects: []knowledgecatalog.Object{object}, CatalogRevision: 2},
		); err == nil {
			t.Fatal("pre-response oversized page succeeded")
		}
	})
}

func assertKnowledgeHTTPListPageRejected(
	t *testing.T,
	request *opensplunk.ListKnowledgeObjectsRequest,
	page knowledgecatalog.ListPage,
) {
	t.Helper()
	catalog := &knowledgeHTTPCatalog{listFn: func(
		context.Context,
		knowledgecatalog.ReadScope,
		knowledgecatalog.ListRequest,
	) (knowledgecatalog.ListPage, error) {
		return page, nil
	}}
	appender := &knowledgeBoundaryAppender{}
	_, handler := newKnowledgeHTTPHandler(
		t,
		auth.BrowserRoleAdministrator,
		catalog,
		&knowledgeHTTPWriter{},
		knowledgeHTTPApps(),
		appender,
	)
	response := knowledgeHTTPPost(t, handler, knowledgeObjectsListPath, request)
	attempts := appender.snapshot()
	if response.Code != http.StatusServiceUnavailable ||
		response.Body.String() != knowledgeManagementUnavailableBody ||
		len(attempts) != 1 ||
		attempts[0].definition.Action != knowledgeattemptaudit.ActionList ||
		attempts[0].definition.Reason != knowledgeattemptaudit.ReasonServiceUnavailable ||
		attempts[0].definition.AuthorizedContext != nil {
		t.Fatalf("status=%d body=%q attempts=%+v", response.Code, response.Body.String(), attempts)
	}
}

func knowledgeListResponseScopes() knowledgeScopes {
	return knowledgeScopes{
		read: knowledgecatalog.ReadScope{
			TenantID:       knowledgeBoundaryTenantID,
			OwnerID:        knowledgeBoundaryOwnerID,
			ReadableAppIDs: []string{knowledgeHTTPAppID},
		},
		apps: []string{knowledgeHTTPAppID},
	}
}

func knowledgeListResponseObject(
	t *testing.T,
	objectID string,
	name string,
	objectType knowledgecatalog.ObjectType,
	sharingScope knowledgecatalog.SharingScope,
	appID string,
	ownerID string,
) knowledgecatalog.Object {
	t.Helper()
	created := time.Date(2026, 8, 7, 12, 0, 0, 123456000, time.UTC)
	definition := knowledgeHTTPDefinition(knowledgeListProtoSharingScope(sharingScope))
	definition.AppId = appID
	definition.Name = name
	switch objectType {
	case knowledgecatalog.ObjectTypeFieldExtraction:
		definition.Body = &opensplunk.KnowledgeObjectDefinition_FieldExtraction{
			FieldExtraction: &opensplunk.FieldExtractionDefinition{
				InputField: "_raw",
				Extraction: &opensplunk.FieldExtractionDefinition_Json{
					Json: &opensplunk.JsonFieldExtractionDefinition{
						Path:        "payload.value",
						OutputField: "value",
					},
				},
			},
		}
	case knowledgecatalog.ObjectTypeCalculatedField:
		definition.Body = &opensplunk.KnowledgeObjectDefinition_CalculatedField{
			CalculatedField: &opensplunk.CalculatedFieldDefinition{
				DestinationField: "calculated_value",
				Expression:       "lower(src)",
			},
		}
	}
	return canonicalKnowledgeListResponseObject(t, knowledgecatalog.Object{
		KnowledgeObjectID: objectID,
		TenantID:          knowledgeBoundaryTenantID,
		AppID:             appID,
		OwnerID:           ownerID,
		ObjectType:        objectType,
		Name:              name,
		Version:           1,
		SharingScope:      sharingScope,
		State:             knowledgecatalog.StateDraft,
		Definition:        definition,
		CreatedAt:         created,
		UpdatedAt:         created,
	})
}

func canonicalKnowledgeListResponseObject(
	t *testing.T,
	object knowledgecatalog.Object,
) knowledgecatalog.Object {
	t.Helper()
	normalized, err := knowledgedefinition.Normalize(object.Definition)
	if err != nil {
		t.Fatalf("normalize list fixture: %v", err)
	}
	object.Definition = normalized.Definition
	object.DefinitionSHA256 = append([]byte(nil), normalized.Digest[:]...)
	return object
}

func knowledgeListProtoSharingScope(
	value knowledgecatalog.SharingScope,
) opensplunk.SharingScope {
	switch value {
	case knowledgecatalog.SharingScopePrivate:
		return opensplunk.SharingScope_SHARING_SCOPE_PRIVATE
	case knowledgecatalog.SharingScopeApp:
		return opensplunk.SharingScope_SHARING_SCOPE_APP
	case knowledgecatalog.SharingScopeGlobal:
		return opensplunk.SharingScope_SHARING_SCOPE_GLOBAL
	default:
		return opensplunk.SharingScope_SHARING_SCOPE_UNSPECIFIED
	}
}
