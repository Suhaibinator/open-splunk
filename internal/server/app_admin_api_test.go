package server

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/auth"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

const appAdministrationBearerToken = "open-splunk-app-administrator-test-token-0123456789"

var appAdministrationCursorKey = bytes.Repeat([]byte{0x5a}, 32)

type fakeAppAdministration struct {
	mu sync.Mutex

	createCalls int
	getCalls    int
	listCalls   int
	updateCalls int
	stateCalls  int
	deleteCalls int

	createFn func(
		context.Context,
		AppAdministrationScope,
		AppAdministrationDefinition,
	) (AppAdministrationWorkspace, error)
	getFn func(
		context.Context,
		AppAdministrationScope,
		AppAdministrationSelector,
	) (AppAdministrationWorkspace, error)
	listFn func(
		context.Context,
		AppAdministrationScope,
		AppAdministrationListRequest,
	) (AppAdministrationListResult, error)
	updateFn func(
		context.Context,
		AppAdministrationScope,
		AppAdministrationSelector,
		uint64,
		AppAdministrationDefinition,
	) (AppAdministrationWorkspace, error)
	stateFn func(
		context.Context,
		AppAdministrationScope,
		AppAdministrationSelector,
		uint64,
		AppAdministrationState,
	) (AppAdministrationWorkspace, error)
	deleteFn func(
		context.Context,
		AppAdministrationScope,
		AppAdministrationSelector,
		uint64,
		string,
	) (string, error)
}

func (service *fakeAppAdministration) CreateApp(
	ctx context.Context,
	scope AppAdministrationScope,
	definition AppAdministrationDefinition,
) (AppAdministrationWorkspace, error) {
	service.mu.Lock()
	service.createCalls++
	fn := service.createFn
	service.mu.Unlock()
	if fn == nil {
		return AppAdministrationWorkspace{}, errors.New("unexpected CreateApp")
	}
	return fn(ctx, scope, definition)
}

func (service *fakeAppAdministration) GetApp(
	ctx context.Context,
	scope AppAdministrationScope,
	selector AppAdministrationSelector,
) (AppAdministrationWorkspace, error) {
	service.mu.Lock()
	service.getCalls++
	fn := service.getFn
	service.mu.Unlock()
	if fn == nil {
		return AppAdministrationWorkspace{}, errors.New("unexpected GetApp")
	}
	return fn(ctx, scope, selector)
}

func (service *fakeAppAdministration) ListApps(
	ctx context.Context,
	scope AppAdministrationScope,
	request AppAdministrationListRequest,
) (AppAdministrationListResult, error) {
	service.mu.Lock()
	service.listCalls++
	fn := service.listFn
	service.mu.Unlock()
	if fn == nil {
		return AppAdministrationListResult{}, errors.New("unexpected ListApps")
	}
	return fn(ctx, scope, request)
}

func (service *fakeAppAdministration) UpdateApp(
	ctx context.Context,
	scope AppAdministrationScope,
	selector AppAdministrationSelector,
	version uint64,
	definition AppAdministrationDefinition,
) (AppAdministrationWorkspace, error) {
	service.mu.Lock()
	service.updateCalls++
	fn := service.updateFn
	service.mu.Unlock()
	if fn == nil {
		return AppAdministrationWorkspace{}, errors.New("unexpected UpdateApp")
	}
	return fn(ctx, scope, selector, version, definition)
}

func (service *fakeAppAdministration) SetAppState(
	ctx context.Context,
	scope AppAdministrationScope,
	selector AppAdministrationSelector,
	version uint64,
	state AppAdministrationState,
) (AppAdministrationWorkspace, error) {
	service.mu.Lock()
	service.stateCalls++
	fn := service.stateFn
	service.mu.Unlock()
	if fn == nil {
		return AppAdministrationWorkspace{}, errors.New("unexpected SetAppState")
	}
	return fn(ctx, scope, selector, version, state)
}

func (service *fakeAppAdministration) DeleteApp(
	ctx context.Context,
	scope AppAdministrationScope,
	selector AppAdministrationSelector,
	version uint64,
	confirmation string,
) (string, error) {
	service.mu.Lock()
	service.deleteCalls++
	fn := service.deleteFn
	service.mu.Unlock()
	if fn == nil {
		return "", errors.New("unexpected DeleteApp")
	}
	return fn(ctx, scope, selector, version, confirmation)
}

func (service *fakeAppAdministration) calls() [6]int {
	service.mu.Lock()
	defer service.mu.Unlock()
	return [6]int{
		service.createCalls,
		service.getCalls,
		service.listCalls,
		service.updateCalls,
		service.stateCalls,
		service.deleteCalls,
	}
}

func TestAppAdministrationCreateDerivesScopeAndProjectsEveryField(
	t *testing.T,
) {
	t.Parallel()

	description := " workspace description "
	earliest := "-24h"
	timezone := "UTC"
	service := &fakeAppAdministration{}
	service.createFn = func(
		ctx context.Context,
		scope AppAdministrationScope,
		definition AppAdministrationDefinition,
	) (AppAdministrationWorkspace, error) {
		if err := ctx.Err(); err != nil {
			t.Fatalf("CreateApp context = %v", err)
		}
		if scope != (AppAdministrationScope{
			TenantID: browserGateTenantID,
			ActorID:  browserGateOwnerID,
		}) {
			t.Fatalf("CreateApp scope = %#v", scope)
		}
		want := AppAdministrationDefinition{
			Slug:              "grade_this",
			DisplayName:       "Grade This",
			Description:       new("workspace description"),
			DefaultIndexNames: []string{"audit", "main"},
			DefaultTimeRange: &AppAdministrationTimeRange{
				Earliest: new(earliest),
				Timezone: new(timezone),
			},
		}
		if !reflect.DeepEqual(definition, want) {
			t.Fatalf("CreateApp definition = %#v, want %#v", definition, want)
		}
		return appAdministrationFixture(
			"app_0123456789ABCDEFGHIJKL",
			1,
			AppAdministrationStateActive,
			definition,
		), nil
	}
	handler := newAppAdministrationTestHandler(t, service, BootstrapConfig{})

	response := postAppAdministrationProto(
		t,
		handler,
		"/api/apps/create",
		&opensplunk.CreateAppRequest{
			Definition: &opensplunk.AppDefinition{
				Slug:              " Grade_This ",
				DisplayName:       " Grade This ",
				Description:       &description,
				DefaultIndexNames: []string{"main", " AUDIT ", "main"},
				DefaultTimeRange: &opensplunk.TimeRangeSpec{
					Earliest: &earliest,
					Timezone: &timezone,
				},
			},
		},
	)
	if response.Code != http.StatusOK {
		t.Fatalf("create status = %d, body = %s", response.Code, response.Body)
	}
	var decoded opensplunk.CreateAppResponse
	unmarshalResponse(t, response, &decoded)
	app := decoded.GetApp()
	if app.GetAppId() != "app_0123456789ABCDEFGHIJKL" ||
		app.GetVersion() != 1 ||
		app.GetState() != opensplunk.AppState_APP_STATE_ACTIVE ||
		app.GetCreatedAt() == nil ||
		app.GetUpdatedAt() == nil {
		t.Fatalf("created app projection = %+v", app)
	}
	definition := app.GetDefinition()
	if definition.GetSlug() != "grade_this" ||
		definition.GetDisplayName() != "Grade This" ||
		definition.Description == nil ||
		definition.GetDescription() != "workspace description" ||
		!slices.Equal(
			definition.GetDefaultIndexNames(),
			[]string{"audit", "main"},
		) ||
		definition.GetDefaultTimeRange() == nil ||
		definition.GetDefaultTimeRange().Earliest == nil ||
		definition.GetDefaultTimeRange().Latest != nil ||
		definition.GetDefaultTimeRange().Timezone == nil {
		t.Fatalf("created definition projection = %+v", definition)
	}
}

func TestAppAdministrationListUsesBoundedRevisionKeysetPaging(
	t *testing.T,
) {
	t.Parallel()

	description := "listed"
	firstRecord := appAdministrationFixture(
		"app_0123456789ABCDEFGHIJKL",
		3,
		AppAdministrationStateActive,
		AppAdministrationDefinition{
			Slug:        "alpha",
			DisplayName: "Same",
			Description: &description,
		},
	)
	secondRecord := appAdministrationFixture(
		"app_ABCDEFGHIJKLMNOPQRSTUV",
		2,
		AppAdministrationStateArchived,
		AppAdministrationDefinition{
			Slug:        "bravo",
			DisplayName: "Same",
		},
	)
	var secondRequest AppAdministrationListRequest
	service := &fakeAppAdministration{}
	service.listFn = func(
		_ context.Context,
		scope AppAdministrationScope,
		request AppAdministrationListRequest,
	) (AppAdministrationListResult, error) {
		if scope.TenantID != browserGateTenantID ||
			scope.ActorID != browserGateOwnerID {
			t.Fatalf("list scope = %#v", scope)
		}
		if request.RequiredCatalogRevision == nil {
			if request.PageSize != 2 ||
				request.PageCursor != "" ||
				!request.IncludeTotal ||
				!slices.Equal(
					request.StateFilters,
					[]AppAdministrationState{
						AppAdministrationStateActive,
						AppAdministrationStateArchived,
					},
				) ||
				request.TextFilter != "same" ||
				request.SortBy != AppAdministrationSortUpdatedAt ||
				!request.SortDescending {
				t.Fatalf("first list request = %#v", request)
			}
			total := uint64(2)
			return AppAdministrationListResult{
				Apps:            []AppAdministrationWorkspace{firstRecord},
				CatalogRevision: 17,
				NextPageCursor:  "updated:opaque-keyset",
				TotalSize:       &total,
				TotalSizeExact:  true,
			}, nil
		}
		secondRequest = request
		return AppAdministrationListResult{
			Apps:            []AppAdministrationWorkspace{secondRecord},
			CatalogRevision: 17,
			TotalSize:       func() *uint64 { value := uint64(2); return &value }(),
			TotalSizeExact:  true,
		}, nil
	}
	handler := newAppAdministrationTestHandler(t, service, BootstrapConfig{})
	size := uint32(2)
	filter := " same "
	request := &opensplunk.ListAppsRequest{
		Page: &opensplunk.PageRequest{
			PageSize:         &size,
			IncludeTotalSize: true,
		},
		StateFilters: []opensplunk.AppState{
			opensplunk.AppState_APP_STATE_ARCHIVED,
			opensplunk.AppState_APP_STATE_ACTIVE,
		},
		TextFilter:    &filter,
		SortBy:        opensplunk.AppSortBy_APP_SORT_BY_UPDATED_AT,
		SortDirection: opensplunk.SortDirection_SORT_DIRECTION_DESCENDING,
	}
	first := postAppAdministrationProto(
		t,
		handler,
		"/api/apps/list",
		request,
	)
	if first.Code != http.StatusOK {
		t.Fatalf("first list status = %d, body = %s", first.Code, first.Body)
	}
	var firstPage opensplunk.ListAppsResponse
	unmarshalResponse(t, first, &firstPage)
	if len(firstPage.GetApps()) != 1 ||
		firstPage.GetApps()[0].GetAppId() != firstRecord.AppID ||
		firstPage.GetPage().NextPageToken == nil ||
		firstPage.GetPage().GetTotalSize() != 2 ||
		!firstPage.GetPage().GetTotalSizeExact() ||
		strings.Contains(
			firstPage.GetPage().GetNextPageToken(),
			"updated:opaque-keyset",
		) {
		t.Fatalf("first list response = %+v", &firstPage)
	}

	token := firstPage.GetPage().GetNextPageToken()
	request.Page.PageToken = &token
	second := postAppAdministrationProto(
		t,
		handler,
		"/api/apps/list",
		request,
	)
	if second.Code != http.StatusOK {
		t.Fatalf("second list status = %d, body = %s", second.Code, second.Body)
	}
	if secondRequest.RequiredCatalogRevision == nil ||
		*secondRequest.RequiredCatalogRevision != 17 ||
		secondRequest.PageCursor != "updated:opaque-keyset" {
		t.Fatalf("second service request = %#v", secondRequest)
	}
	var secondPage opensplunk.ListAppsResponse
	unmarshalResponse(t, second, &secondPage)
	if len(secondPage.GetApps()) != 1 ||
		secondPage.GetApps()[0].GetAppId() != secondRecord.AppID ||
		secondPage.GetPage().NextPageToken != nil {
		t.Fatalf("second list response = %+v", &secondPage)
	}

	changedFilter := "different"
	request.TextFilter = &changedFilter
	filterMismatch := postAppAdministrationProto(
		t,
		handler,
		"/api/apps/list",
		request,
	)
	if filterMismatch.Code != http.StatusBadRequest ||
		!strings.Contains(
			filterMismatch.Body.String(),
			"page token is invalid",
		) {
		t.Fatalf(
			"filter-mismatched cursor = %d, %s",
			filterMismatch.Code,
			filterMismatch.Body,
		)
	}
	request.TextFilter = &filter
	tampered := token[:len(token)-1] + "A"
	request.Page.PageToken = &tampered
	rejected := postAppAdministrationProto(
		t,
		handler,
		"/api/apps/list",
		request,
	)
	if rejected.Code != http.StatusBadRequest ||
		!strings.Contains(rejected.Body.String(), "page token is invalid") {
		t.Fatalf("tampered cursor = %d, %s", rejected.Code, rejected.Body)
	}
}

func TestAppAdministrationUpdateStateAndDeleteSemantics(t *testing.T) {
	t.Parallel()

	current := appAdministrationFixture(
		"app_0123456789ABCDEFGHIJKL",
		1,
		AppAdministrationStateActive,
		AppAdministrationDefinition{
			Slug:              "immutable",
			DisplayName:       "Before",
			Description:       new("remove me"),
			DefaultIndexNames: []string{"main"},
			DefaultTimeRange: &AppAdministrationTimeRange{
				Earliest: new("-24h"),
				Latest:   new("now"),
			},
		},
	)
	service := &fakeAppAdministration{}
	service.getFn = func(
		_ context.Context,
		scope AppAdministrationScope,
		selector AppAdministrationSelector,
	) (AppAdministrationWorkspace, error) {
		if scope != (AppAdministrationScope{
			TenantID: browserGateTenantID,
			ActorID:  browserGateOwnerID,
		}) {
			t.Fatalf("GetApp scope = %#v", scope)
		}
		if selector != (AppAdministrationSelector{Slug: "immutable"}) &&
			selector != (AppAdministrationSelector{AppID: current.AppID}) {
			t.Fatalf("GetApp selector = %#v", selector)
		}
		return current, nil
	}
	service.updateFn = func(
		_ context.Context,
		scope AppAdministrationScope,
		selector AppAdministrationSelector,
		version uint64,
		replacement AppAdministrationDefinition,
	) (AppAdministrationWorkspace, error) {
		if scope.ActorID != browserGateOwnerID ||
			selector != (AppAdministrationSelector{AppID: current.AppID}) ||
			version != 1 {
			t.Fatalf(
				"UpdateApp call = scope %#v selector %#v version %d",
				scope,
				selector,
				version,
			)
		}
		want := cloneAppAdministrationDefinition(current.Definition)
		want.DisplayName = "After"
		want.Description = nil
		if !reflect.DeepEqual(replacement, want) {
			t.Fatalf("UpdateApp replacement = %#v, want %#v", replacement, want)
		}
		updated := current
		updated.Version = 2
		updated.Definition = replacement
		updated.UpdatedAt = current.UpdatedAt.Add(time.Second)
		current = updated
		return updated, nil
	}
	handler := newAppAdministrationTestHandler(t, service, BootstrapConfig{})
	update := postAppAdministrationProto(
		t,
		handler,
		"/api/apps/update",
		&opensplunk.UpdateAppRequest{
			Selector: &opensplunk.AppSelector{
				Selector: &opensplunk.AppSelector_Slug{Slug: "immutable"},
			},
			ExpectedVersion: 1,
			Definition: &opensplunk.AppDefinition{
				Slug:        "immutable",
				DisplayName: " After ",
			},
			UpdateMask: &fieldmaskpb.FieldMask{
				Paths: []string{"display_name", "description"},
			},
		},
	)
	if update.Code != http.StatusOK {
		t.Fatalf("update status = %d, body = %s", update.Code, update.Body)
	}
	var updated opensplunk.UpdateAppResponse
	unmarshalResponse(t, update, &updated)
	if updated.GetApp().GetVersion() != 2 ||
		updated.GetApp().GetDefinition().GetDisplayName() != "After" ||
		updated.GetApp().GetDefinition().Description != nil ||
		updated.GetApp().GetDefinition().GetDefaultTimeRange() == nil {
		t.Fatalf("updated app = %+v", updated.GetApp())
	}

	service.stateFn = func(
		_ context.Context,
		_ AppAdministrationScope,
		selector AppAdministrationSelector,
		version uint64,
		state AppAdministrationState,
	) (AppAdministrationWorkspace, error) {
		if selector != (AppAdministrationSelector{AppID: current.AppID}) ||
			version != 2 ||
			state != AppAdministrationStateArchived {
			t.Fatalf(
				"SetAppState call = %#v, %d, %s",
				selector,
				version,
				state,
			)
		}
		current.Version = 3
		current.State = state
		current.UpdatedAt = current.UpdatedAt.Add(time.Second)
		return current, nil
	}
	state := postAppAdministrationProto(
		t,
		handler,
		"/api/apps/state/set",
		&opensplunk.SetAppStateRequest{
			Selector: &opensplunk.AppSelector{
				Selector: &opensplunk.AppSelector_Slug{Slug: "immutable"},
			},
			ExpectedVersion: 2,
			State:           opensplunk.AppState_APP_STATE_ARCHIVED,
		},
	)
	if state.Code != http.StatusOK {
		t.Fatalf("state status = %d, body = %s", state.Code, state.Body)
	}
	var archived opensplunk.SetAppStateResponse
	unmarshalResponse(t, state, &archived)
	if archived.GetApp().GetVersion() != 3 ||
		archived.GetApp().GetState() !=
			opensplunk.AppState_APP_STATE_ARCHIVED {
		t.Fatalf("archived app = %+v", archived.GetApp())
	}

	service.deleteFn = func(
		_ context.Context,
		_ AppAdministrationScope,
		selector AppAdministrationSelector,
		version uint64,
		confirmation string,
	) (string, error) {
		if selector != (AppAdministrationSelector{AppID: current.AppID}) ||
			version != 3 ||
			confirmation != "immutable" {
			t.Fatalf(
				"DeleteApp call = %#v, %d, %q",
				selector,
				version,
				confirmation,
			)
		}
		return current.AppID, nil
	}
	deleted := postAppAdministrationProto(
		t,
		handler,
		"/api/apps/delete",
		&opensplunk.DeleteAppRequest{
			Selector: &opensplunk.AppSelector{
				Selector: &opensplunk.AppSelector_Slug{Slug: "immutable"},
			},
			ExpectedVersion:  3,
			ConfirmationSlug: "immutable",
		},
	)
	if deleted.Code != http.StatusOK {
		t.Fatalf("delete status = %d, body = %s", deleted.Code, deleted.Body)
	}
	var deleteResponse opensplunk.DeleteAppResponse
	unmarshalResponse(t, deleted, &deleteResponse)
	if deleteResponse.GetAppId() != current.AppID {
		t.Fatalf("deleted ID = %q", deleteResponse.GetAppId())
	}
}

func TestAppAdministrationSetStateUsesDetachedDefinitionOracle(t *testing.T) {
	t.Parallel()

	description := "Before"
	earliest := "-24h"
	latest := "now"
	timezone := "UTC"
	stored := appAdministrationFixture(
		"app_0123456789ABCDEFGHIJKL",
		1,
		AppAdministrationStateActive,
		AppAdministrationDefinition{
			Slug:              "aliased",
			DisplayName:       "Aliased",
			Description:       &description,
			DefaultIndexNames: []string{"main", "secondary"},
			DefaultTimeRange: &AppAdministrationTimeRange{
				Earliest: &earliest,
				Latest:   &latest,
				Timezone: &timezone,
			},
		},
	)
	service := &fakeAppAdministration{
		getFn: func(
			context.Context,
			AppAdministrationScope,
			AppAdministrationSelector,
		) (AppAdministrationWorkspace, error) {
			// Returning a value copy intentionally retains aliases to every
			// nested mutable definition field.
			return stored, nil
		},
		stateFn: func(
			_ context.Context,
			_ AppAdministrationScope,
			_ AppAdministrationSelector,
			_ uint64,
			state AppAdministrationState,
		) (AppAdministrationWorkspace, error) {
			*stored.Definition.Description = "Changed"
			stored.Definition.DefaultIndexNames[0] = "other"
			*stored.Definition.DefaultTimeRange.Earliest = "-48h"
			*stored.Definition.DefaultTimeRange.Latest = "-1h"
			*stored.Definition.DefaultTimeRange.Timezone = "Etc/UTC"
			stored.Version = 2
			stored.State = state
			stored.UpdatedAt = stored.UpdatedAt.Add(time.Second)
			return stored, nil
		},
	}
	handler := newAppAdministrationTestHandler(t, service, BootstrapConfig{})
	response := postAppAdministrationProto(
		t,
		handler,
		"/api/apps/state/set",
		&opensplunk.SetAppStateRequest{
			Selector: &opensplunk.AppSelector{
				Selector: &opensplunk.AppSelector_AppId{
					AppId: stored.AppID,
				},
			},
			ExpectedVersion: 1,
			State:           opensplunk.AppState_APP_STATE_ARCHIVED,
		},
	)
	if response.Code != http.StatusInternalServerError ||
		strings.Contains(response.Body.String(), "Changed") ||
		service.calls()[4] != 1 {
		t.Fatalf(
			"aliased state result = %d, %s, calls %v",
			response.Code,
			response.Body,
			service.calls(),
		)
	}
}

func TestAppAdministrationDeleteFailsClosedBeforeDestructiveCall(
	t *testing.T,
) {
	t.Parallel()

	active := appAdministrationFixture(
		"app_0123456789ABCDEFGHIJKL",
		2,
		AppAdministrationStateActive,
		AppAdministrationDefinition{
			Slug:        "protected",
			DisplayName: "Protected",
		},
	)
	service := &fakeAppAdministration{
		getFn: func(
			_ context.Context,
			_ AppAdministrationScope,
			_ AppAdministrationSelector,
		) (AppAdministrationWorkspace, error) {
			return active, nil
		},
		deleteFn: func(
			context.Context,
			AppAdministrationScope,
			AppAdministrationSelector,
			uint64,
			string,
		) (string, error) {
			t.Fatal("DeleteApp reached for rejected request")
			return "", nil
		},
	}
	handler := newAppAdministrationTestHandler(t, service, BootstrapConfig{})
	request := &opensplunk.DeleteAppRequest{
		Selector: &opensplunk.AppSelector{
			Selector: &opensplunk.AppSelector_AppId{AppId: active.AppID},
		},
		ExpectedVersion:  2,
		ConfirmationSlug: "protected",
	}
	response := postAppAdministrationProto(
		t,
		handler,
		"/api/apps/delete",
		request,
	)
	if response.Code != http.StatusConflict {
		t.Fatalf("active delete = %d, %s", response.Code, response.Body)
	}
	if service.calls()[5] != 0 {
		t.Fatalf("active delete calls = %v", service.calls())
	}

	active.State = AppAdministrationStateArchived
	request.ConfirmationSlug = "wrong"
	response = postAppAdministrationProto(
		t,
		handler,
		"/api/apps/delete",
		request,
	)
	if response.Code != http.StatusBadRequest ||
		strings.Contains(response.Body.String(), active.Definition.Slug) {
		t.Fatalf("wrong confirmation = %d, %s", response.Code, response.Body)
	}
	if service.calls()[5] != 0 {
		t.Fatalf("wrong confirmation delete calls = %v", service.calls())
	}
}

func TestAppAdministrationImmutableSlugAndVersionConflicts(t *testing.T) {
	t.Parallel()

	current := appAdministrationFixture(
		"app_0123456789ABCDEFGHIJKL",
		3,
		AppAdministrationStateActive,
		AppAdministrationDefinition{
			Slug:        "stable",
			DisplayName: "Stable",
		},
	)
	service := &fakeAppAdministration{
		getFn: func(
			context.Context,
			AppAdministrationScope,
			AppAdministrationSelector,
		) (AppAdministrationWorkspace, error) {
			return current, nil
		},
		updateFn: func(
			context.Context,
			AppAdministrationScope,
			AppAdministrationSelector,
			uint64,
			AppAdministrationDefinition,
		) (AppAdministrationWorkspace, error) {
			t.Fatal("UpdateApp reached for rejected request")
			return AppAdministrationWorkspace{}, nil
		},
		stateFn: func(
			context.Context,
			AppAdministrationScope,
			AppAdministrationSelector,
			uint64,
			AppAdministrationState,
		) (AppAdministrationWorkspace, error) {
			t.Fatal("SetAppState reached for rejected request")
			return AppAdministrationWorkspace{}, nil
		},
	}
	handler := newAppAdministrationTestHandler(t, service, BootstrapConfig{})
	base := &opensplunk.UpdateAppRequest{
		Selector: &opensplunk.AppSelector{
			Selector: &opensplunk.AppSelector_AppId{AppId: current.AppID},
		},
		ExpectedVersion: 3,
		Definition: &opensplunk.AppDefinition{
			Slug:        "renamed",
			DisplayName: "Stable",
		},
	}
	response := postAppAdministrationProto(
		t,
		handler,
		"/api/apps/update",
		base,
	)
	if response.Code != http.StatusConflict {
		t.Fatalf("full rename = %d, %s", response.Code, response.Body)
	}

	base.Definition.Slug = "stable"
	base.UpdateMask = &fieldmaskpb.FieldMask{Paths: []string{"slug"}}
	response = postAppAdministrationProto(
		t,
		handler,
		"/api/apps/update",
		base,
	)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("masked slug = %d, %s", response.Code, response.Body)
	}

	base.UpdateMask = nil
	base.ExpectedVersion = 2
	response = postAppAdministrationProto(
		t,
		handler,
		"/api/apps/update",
		base,
	)
	if response.Code != http.StatusConflict {
		t.Fatalf("stale update = %d, %s", response.Code, response.Body)
	}
	if service.calls()[3] != 0 {
		t.Fatalf("rejected updates reached mutation = %v", service.calls())
	}
	state := postAppAdministrationProto(
		t,
		handler,
		"/api/apps/state/set",
		&opensplunk.SetAppStateRequest{
			Selector: &opensplunk.AppSelector{
				Selector: &opensplunk.AppSelector_Slug{Slug: "stable"},
			},
			ExpectedVersion: 2,
			State:           opensplunk.AppState_APP_STATE_ARCHIVED,
		},
	)
	if state.Code != http.StatusConflict ||
		service.calls()[4] != 0 {
		t.Fatalf(
			"stale state = %d, %s, calls %v",
			state.Code,
			state.Body,
			service.calls(),
		)
	}
}

type appAdministrationAuthenticator struct {
	mu        sync.Mutex
	calls     int
	principal auth.BrowserPrincipal
	err       error
}

func (authenticator *appAdministrationAuthenticator) Authenticate(
	_ context.Context,
	_ []byte,
) (auth.BrowserPrincipal, error) {
	authenticator.mu.Lock()
	defer authenticator.mu.Unlock()
	authenticator.calls++
	return authenticator.principal, authenticator.err
}

func (authenticator *appAdministrationAuthenticator) callCount() int {
	authenticator.mu.Lock()
	defer authenticator.mu.Unlock()
	return authenticator.calls
}

func TestAppAdministrationRequestBoundaryOrderAndExactness(t *testing.T) {
	t.Parallel()

	service := &fakeAppAdministration{}
	authenticator := &appAdministrationAuthenticator{
		principal: browserGatePrincipal(
			t,
			browserGateTenantID,
			browserGateOwnerID,
			auth.BrowserRoleAdministrator,
		),
	}
	handler := newAppAdministrationHandlerWithAuthenticator(
		t,
		service,
		authenticator,
	)

	method := httptest.NewRequestWithContext(
		context.Background(),
		http.MethodGet,
		"/api/apps/create",
		nil,
	)
	method.Host = "example.com"
	method.Header.Set(
		"Authorization",
		"Bearer "+appAdministrationBearerToken,
	)
	methodResponse := httptest.NewRecorder()
	handler.ServeHTTP(methodResponse, method)
	if methodResponse.Code != http.StatusMethodNotAllowed ||
		authenticator.callCount() != 0 {
		t.Fatalf(
			"method boundary = %d, auth calls %d",
			methodResponse.Code,
			authenticator.callCount(),
		)
	}

	unknown := httptest.NewRequestWithContext(
		context.Background(),
		http.MethodPost,
		"/api/apps/create/",
		nil,
	)
	unknown.Host = "example.com"
	unknown.Header.Set(
		"Authorization",
		"Bearer "+appAdministrationBearerToken,
	)
	unknownResponse := httptest.NewRecorder()
	handler.ServeHTTP(unknownResponse, unknown)
	if unknownResponse.Code != http.StatusNotFound ||
		authenticator.callCount() != 0 {
		t.Fatalf(
			"path boundary = %d, auth calls %d",
			unknownResponse.Code,
			authenticator.callCount(),
		)
	}

	untrusted := httptest.NewRequestWithContext(
		context.Background(),
		http.MethodPost,
		"/api/apps/create",
		strings.NewReader("malformed"),
	)
	untrusted.Host = "attacker.example"
	untrusted.Header.Set(
		"Authorization",
		"Bearer "+appAdministrationBearerToken,
	)
	untrustedResponse := httptest.NewRecorder()
	handler.ServeHTTP(untrustedResponse, untrusted)
	if untrustedResponse.Code != http.StatusForbidden ||
		authenticator.callCount() != 0 {
		t.Fatalf(
			"origin boundary = %d, auth calls %d",
			untrustedResponse.Code,
			authenticator.callCount(),
		)
	}

	noCredential := httptest.NewRequestWithContext(
		context.Background(),
		http.MethodPost,
		"/api/apps/create",
		strings.NewReader("malformed"),
	)
	noCredential.Host = "example.com"
	noCredentialResponse := httptest.NewRecorder()
	handler.ServeHTTP(noCredentialResponse, noCredential)
	if noCredentialResponse.Code != http.StatusUnauthorized ||
		authenticator.callCount() != 0 {
		t.Fatalf(
			"auth boundary = %d, auth calls %d",
			noCredentialResponse.Code,
			authenticator.callCount(),
		)
	}

	wrongContent := httptest.NewRequestWithContext(
		context.Background(),
		http.MethodPost,
		"/api/apps/create",
		strings.NewReader("malformed"),
	)
	wrongContent.Host = "example.com"
	wrongContent.Header.Set(
		"Authorization",
		"Bearer "+appAdministrationBearerToken,
	)
	wrongContent.Header.Set("Content-Type", "application/json")
	wrongContentResponse := httptest.NewRecorder()
	handler.ServeHTTP(wrongContentResponse, wrongContent)
	if wrongContentResponse.Code != http.StatusUnsupportedMediaType ||
		authenticator.callCount() != 1 {
		t.Fatalf(
			"content boundary = %d, auth calls %d",
			wrongContentResponse.Code,
			authenticator.callCount(),
		)
	}

	malformed := httptest.NewRequestWithContext(
		context.Background(),
		http.MethodPost,
		"/api/apps/create",
		strings.NewReader("malformed"),
	)
	malformed.Host = "example.com"
	malformed.Header.Set(
		"Authorization",
		"Bearer "+appAdministrationBearerToken,
	)
	malformed.Header.Set("Content-Type", "application/x-protobuf")
	malformedResponse := httptest.NewRecorder()
	handler.ServeHTTP(malformedResponse, malformed)
	if malformedResponse.Code != http.StatusBadRequest ||
		authenticator.callCount() != 2 ||
		service.calls() != ([6]int{}) {
		t.Fatalf(
			"decode boundary = %d, auth %d, service %v",
			malformedResponse.Code,
			authenticator.callCount(),
			service.calls(),
		)
	}
}

func TestAppAdministrationDiscardsUnknownFieldsRecursively(t *testing.T) {
	t.Parallel()

	record := appAdministrationFixture(
		"app_valid",
		1,
		AppAdministrationStateActive,
		AppAdministrationDefinition{Slug: "valid", DisplayName: "Valid"},
	)
	service := &fakeAppAdministration{
		getFn: func(
			context.Context,
			AppAdministrationScope,
			AppAdministrationSelector,
		) (AppAdministrationWorkspace, error) {
			return record, nil
		},
		listFn: func(
			context.Context,
			AppAdministrationScope,
			AppAdministrationListRequest,
		) (AppAdministrationListResult, error) {
			return AppAdministrationListResult{}, nil
		},
	}
	handler := newAppAdministrationTestHandler(t, service, BootstrapConfig{})
	unknown := futureProtobufField("future-app-field")
	topLevel := &opensplunk.GetAppRequest{
		Selector: &opensplunk.AppSelector{
			Selector: &opensplunk.AppSelector_AppId{AppId: "app_valid"},
		},
	}
	topLevel.ProtoReflect().SetUnknown(unknown)
	nested := proto.Clone(topLevel).(*opensplunk.GetAppRequest)
	nested.ProtoReflect().SetUnknown(nil)
	nested.Selector.ProtoReflect().SetUnknown(unknown)
	page := &opensplunk.ListAppsRequest{
		Page: &opensplunk.PageRequest{},
	}
	page.Page.ProtoReflect().SetUnknown(unknown)

	for name, test := range map[string]struct {
		path    string
		request proto.Message
	}{
		"top level": {"/api/apps/get", topLevel},
		"selector":  {"/api/apps/get", nested},
		"page":      {"/api/apps/list", page},
	} {
		t.Run(name, func(t *testing.T) {
			response := postAppAdministrationProto(
				t,
				handler,
				test.path,
				test.request,
			)
			if response.Code != http.StatusOK {
				t.Fatalf(
					"status = %d, body = %s",
					response.Code,
					response.Body,
				)
			}
		})
	}
	if service.calls() != ([6]int{0, 2, 1, 0, 0, 0}) {
		t.Fatalf("service calls = %v", service.calls())
	}

	unknownOnlySelector := &opensplunk.GetAppRequest{
		Selector: &opensplunk.AppSelector{},
	}
	unknownOnlySelector.Selector.ProtoReflect().SetUnknown(unknown)
	response := postAppAdministrationProto(
		t,
		handler,
		"/api/apps/get",
		unknownOnlySelector,
	)
	if response.Code != http.StatusBadRequest {
		t.Fatalf(
			"unknown-only selector status = %d, body = %s",
			response.Code,
			response.Body,
		)
	}
	if service.calls() != ([6]int{0, 2, 1, 0, 0, 0}) {
		t.Fatalf("unknown-only selector reached service: %v", service.calls())
	}
}

func TestEveryAppAdministrationRouteRejectsOrdinaryPrincipal(t *testing.T) {
	t.Parallel()

	service := &fakeAppAdministration{}
	handler := newAppAdministrationHandlerWithAuthenticator(
		t,
		service,
		&appAdministrationAuthenticator{
			principal: browserGatePrincipal(
				t,
				browserGateTenantID,
				browserGateOwnerID,
				auth.BrowserRoleUser,
			),
		},
	)
	for _, path := range []string{
		"/api/apps/create",
		"/api/apps/get",
		"/api/apps/list",
		"/api/apps/update",
		"/api/apps/state/set",
		"/api/apps/delete",
	} {
		request := httptest.NewRequestWithContext(
			context.Background(),
			http.MethodPost,
			path,
			nil,
		)
		request.Host = "example.com"
		request.Header.Set(
			"Authorization",
			"Bearer "+appAdministrationBearerToken,
		)
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusForbidden {
			t.Fatalf(
				"%s status = %d, body = %s",
				path,
				response.Code,
				response.Body,
			)
		}
		unauthenticated := httptest.NewRequestWithContext(
			context.Background(),
			http.MethodPost,
			path,
			strings.NewReader("malformed"),
		)
		unauthenticated.Host = "example.com"
		unauthenticatedResponse := httptest.NewRecorder()
		handler.ServeHTTP(
			unauthenticatedResponse,
			unauthenticated,
		)
		if unauthenticatedResponse.Code != http.StatusUnauthorized {
			t.Fatalf(
				"%s unauthenticated status = %d, body = %s",
				path,
				unauthenticatedResponse.Code,
				unauthenticatedResponse.Body,
			)
		}
	}
	if service.calls() != ([6]int{}) {
		t.Fatalf("ordinary principal reached app service: %v", service.calls())
	}
}

func TestAppAdministrationTimeRangePreservesIndependentPresence(
	t *testing.T,
) {
	t.Parallel()

	earliest := "-24h"
	latest := "now"
	timezone := "America/Los_Angeles"
	futureAbsolute := "2026-01-01T00:00:00Z"
	tests := []struct {
		name  string
		input *opensplunk.TimeRangeSpec
		want  *AppAdministrationTimeRange
	}{
		{name: "absent"},
		{
			name:  "present empty",
			input: &opensplunk.TimeRangeSpec{},
			want:  &AppAdministrationTimeRange{},
		},
		{
			name:  "earliest only",
			input: &opensplunk.TimeRangeSpec{Earliest: &earliest},
			want: &AppAdministrationTimeRange{
				Earliest: new(earliest),
			},
		},
		{
			name:  "latest only",
			input: &opensplunk.TimeRangeSpec{Latest: &latest},
			want: &AppAdministrationTimeRange{
				Latest: new(latest),
			},
		},
		{
			name:  "timezone only",
			input: &opensplunk.TimeRangeSpec{Timezone: &timezone},
			want: &AppAdministrationTimeRange{
				Timezone: new(timezone),
			},
		},
		{
			name: "all",
			input: &opensplunk.TimeRangeSpec{
				Earliest: &earliest,
				Latest:   &latest,
				Timezone: &timezone,
			},
			want: &AppAdministrationTimeRange{
				Earliest: new(earliest),
				Latest:   new(latest),
				Timezone: new(timezone),
			},
		},
		{
			name: "absolute intent independent of validation wall clock",
			input: &opensplunk.TimeRangeSpec{
				Earliest: &futureAbsolute,
				Latest:   &latest,
			},
			want: &AppAdministrationTimeRange{
				Earliest: new(futureAbsolute),
				Latest:   new(latest),
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// Normalization reads no clock, so a repeated call must
			// return the same detached range.
			first, firstErr := normalizeAppAdministrationTimeRange(
				test.input,
			)
			second, secondErr := normalizeAppAdministrationTimeRange(
				test.input,
			)
			if firstErr != nil ||
				secondErr != nil ||
				!reflect.DeepEqual(first, test.want) ||
				!reflect.DeepEqual(second, test.want) {
				t.Fatalf(
					"ranges = %#v/%v and %#v/%v, want %#v",
					first,
					firstErr,
					second,
					secondErr,
					test.want,
				)
			}
			roundTrip := appAdministrationTimeRangeToProto(first)
			if (test.input == nil) != (roundTrip == nil) ||
				(test.input != nil &&
					!proto.Equal(test.input, roundTrip)) {
				t.Fatalf(
					"round trip = %+v, want %+v",
					roundTrip,
					test.input,
				)
			}
		})
	}
}

func TestAppAdministrationRequestEnvelopeMatchesFieldBounds(t *testing.T) {
	t.Parallel()

	exactDescription := strings.Repeat(
		"d",
		maximumAppAdministrationDescription,
	)
	service := &fakeAppAdministration{
		createFn: func(
			_ context.Context,
			_ AppAdministrationScope,
			definition AppAdministrationDefinition,
		) (AppAdministrationWorkspace, error) {
			return appAdministrationFixture(
				"app_0123456789ABCDEFGHIJKL",
				1,
				AppAdministrationStateActive,
				definition,
			), nil
		},
	}
	handler := newAppAdministrationTestHandler(t, service, BootstrapConfig{})
	exact := &opensplunk.CreateAppRequest{
		Definition: &opensplunk.AppDefinition{
			Slug:        "exact",
			DisplayName: "Exact",
			Description: &exactDescription,
		},
	}
	if proto.Size(exact) <= int(maximumSmallRequestBytes) {
		t.Fatalf(
			"exact valid request = %d bytes, want above small route cap %d",
			proto.Size(exact),
			maximumSmallRequestBytes,
		)
	}
	response := postAppAdministrationProto(
		t,
		handler,
		"/api/apps/create",
		exact,
	)
	if response.Code != http.StatusOK {
		t.Fatalf("exact envelope = %d, %s", response.Code, response.Body)
	}

	tooLong := exactDescription + "x"
	exact.Definition.Description = &tooLong
	response = postAppAdministrationProto(
		t,
		handler,
		"/api/apps/create",
		exact,
	)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("field overflow = %d, %s", response.Code, response.Body)
	}

	outer := httptest.NewRequestWithContext(
		context.Background(),
		http.MethodPost,
		"/api/apps/create",
		bytes.NewReader(make([]byte, maximumRequestBytes+1)),
	)
	outer.Host = "example.com"
	outer.Header.Set("Content-Type", "application/x-protobuf")
	outer.Header.Set(
		"Authorization",
		"Bearer "+appAdministrationBearerToken,
	)
	outerResponse := httptest.NewRecorder()
	handler.ServeHTTP(outerResponse, outer)
	if outerResponse.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf(
			"outer overflow = %d, %s",
			outerResponse.Code,
			outerResponse.Body,
		)
	}
}

func TestAppAdministrationStaleCatalogRevisionIsInvalidPageToken(
	t *testing.T,
) {
	t.Parallel()

	service := &fakeAppAdministration{}
	service.listFn = func(
		_ context.Context,
		_ AppAdministrationScope,
		request AppAdministrationListRequest,
	) (AppAdministrationListResult, error) {
		if request.RequiredCatalogRevision == nil {
			return AppAdministrationListResult{
				Apps: []AppAdministrationWorkspace{
					appAdministrationFixture(
						"app_0123456789ABCDEFGHIJKL",
						1,
						AppAdministrationStateActive,
						AppAdministrationDefinition{
							Slug:        "first",
							DisplayName: "First",
						},
					),
				},
				CatalogRevision: 9,
				NextPageCursor:  "first-keyset",
			}, nil
		}
		return AppAdministrationListResult{},
			ErrAppAdministrationInvalidPageToken
	}
	handler := newAppAdministrationTestHandler(t, service, BootstrapConfig{})
	size := uint32(1)
	request := &opensplunk.ListAppsRequest{
		Page: &opensplunk.PageRequest{PageSize: &size},
	}
	first := postAppAdministrationProto(
		t,
		handler,
		"/api/apps/list",
		request,
	)
	var decoded opensplunk.ListAppsResponse
	unmarshalResponse(t, first, &decoded)
	if decoded.GetPage().NextPageToken == nil {
		t.Fatalf("first page = %+v", &decoded)
	}
	token := decoded.GetPage().GetNextPageToken()
	request.Page.PageToken = &token
	stale := postAppAdministrationProto(
		t,
		handler,
		"/api/apps/list",
		request,
	)
	if stale.Code != http.StatusBadRequest ||
		!strings.Contains(stale.Body.String(), "page token is invalid") {
		t.Fatalf("stale page = %d, %s", stale.Code, stale.Body)
	}
}

func TestAppAdministrationRejectsExplicitEmptyOrNonCanonicalPageToken(
	t *testing.T,
) {
	t.Parallel()

	service := &fakeAppAdministration{
		listFn: func(
			context.Context,
			AppAdministrationScope,
			AppAdministrationListRequest,
		) (AppAdministrationListResult, error) {
			t.Fatal("ListApps reached for an invalid page token")
			return AppAdministrationListResult{}, nil
		},
	}
	handler := newAppAdministrationTestHandler(t, service, BootstrapConfig{})
	tests := []struct {
		name  string
		token string
	}{
		{name: "present empty", token: ""},
		{name: "whitespace only", token: " \t\n "},
		{name: "padded", token: " invalid-token "},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			token := test.token
			response := postAppAdministrationProto(
				t,
				handler,
				"/api/apps/list",
				&opensplunk.ListAppsRequest{
					Page: &opensplunk.PageRequest{
						PageToken: &token,
					},
				},
			)
			if response.Code != http.StatusBadRequest ||
				!strings.Contains(
					response.Body.String(),
					"page token is invalid",
				) {
				t.Fatalf(
					"invalid token %q = %d, %s",
					test.token,
					response.Code,
					response.Body,
				)
			}
		})
	}
	if service.calls()[2] != 0 {
		t.Fatalf("list calls = %v", service.calls())
	}
}

func TestAppAdministrationCursorSurvivesRestartAndClonesSigningKey(
	t *testing.T,
) {
	t.Parallel()

	record := appAdministrationFixture(
		"app_0123456789ABCDEFGHIJKL",
		1,
		AppAdministrationStateActive,
		AppAdministrationDefinition{
			Slug:        "restart",
			DisplayName: "Restart",
		},
	)
	service := &fakeAppAdministration{
		listFn: func(
			_ context.Context,
			_ AppAdministrationScope,
			request AppAdministrationListRequest,
		) (AppAdministrationListResult, error) {
			if request.RequiredCatalogRevision == nil {
				return AppAdministrationListResult{
					Apps:            []AppAdministrationWorkspace{record},
					CatalogRevision: 4,
					NextPageCursor:  "restart-keyset",
				}, nil
			}
			if *request.RequiredCatalogRevision != 4 ||
				request.PageCursor != "restart-keyset" {
				t.Fatalf("restarted request = %#v", request)
			}
			return AppAdministrationListResult{
				CatalogRevision: 4,
			}, nil
		},
	}
	key := bytes.Repeat([]byte{0x7b}, 32)
	firstHandler := newAppAdministrationHandlerWithKey(t, service, key)
	size := uint32(1)
	request := &opensplunk.ListAppsRequest{
		Page: &opensplunk.PageRequest{PageSize: &size},
	}
	first := postAppAdministrationProto(
		t,
		firstHandler,
		"/api/apps/list",
		request,
	)
	var page opensplunk.ListAppsResponse
	unmarshalResponse(t, first, &page)
	token := page.GetPage().GetNextPageToken()
	if token == "" {
		t.Fatalf("first page = %+v", &page)
	}
	// Mutating the caller-owned slice must not mutate the already constructed
	// handler's key, and a fresh handler with the persisted original key must
	// accept the first handler's continuation.
	clear(key)
	request.Page.PageToken = &token
	originalAfterMutation := postAppAdministrationProto(
		t,
		firstHandler,
		"/api/apps/list",
		request,
	)
	if originalAfterMutation.Code != http.StatusOK {
		t.Fatalf(
			"cloned-key page = %d, %s",
			originalAfterMutation.Code,
			originalAfterMutation.Body,
		)
	}
	restartKey := bytes.Repeat([]byte{0x7b}, 32)
	secondHandler := newAppAdministrationHandlerWithKey(
		t,
		service,
		restartKey,
	)
	second := postAppAdministrationProto(
		t,
		secondHandler,
		"/api/apps/list",
		request,
	)
	if second.Code != http.StatusOK {
		t.Fatalf("restart page = %d, %s", second.Code, second.Body)
	}
}

func TestAppAdministrationDescriptionCanonicalizesEmptyToAbsent(
	t *testing.T,
) {
	t.Parallel()

	handler := &apiHandler{now: time.Now}
	for _, input := range []*string{nil, new(""), new(" \n ")} {
		normalized, err := normalizeAppAdministrationDescription(input)
		if err != nil || normalized != nil {
			t.Fatalf("normalize description(%v) = %v, %v", input, normalized, err)
		}
	}
	definition, err := appAdministrationDefinition(
		&opensplunk.AppDefinition{
			Slug:        "empty-description",
			DisplayName: "Empty Description",
			Description: new(""),
		},
	)
	if err != nil || definition.Description != nil {
		t.Fatalf("definition = %#v, %v", definition, err)
	}
	projected, err := handler.appAdministrationDefinitionToProto(definition)
	if err != nil || projected.Description != nil {
		t.Fatalf("projected = %+v, %v", projected, err)
	}
}

func TestAppAdministrationErrorMappingIsFixedAndDetailFree(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		err    error
		status int
	}{
		{
			name:   "invalid",
			err:    ErrAppAdministrationInvalidArgument,
			status: http.StatusBadRequest,
		},
		{
			name:   "not found",
			err:    ErrAppAdministrationNotFound,
			status: http.StatusNotFound,
		},
		{
			name:   "exists",
			err:    ErrAppAdministrationAlreadyExists,
			status: http.StatusConflict,
		},
		{
			name:   "conflict",
			err:    ErrAppAdministrationConflict,
			status: http.StatusConflict,
		},
		{
			name:   "capacity",
			err:    ErrAppAdministrationCapacity,
			status: http.StatusTooManyRequests,
		},
		{
			name:   "canceled",
			err:    context.Canceled,
			status: http.StatusRequestTimeout,
		},
		{
			name:   "unknown",
			err:    errors.New("secret-storage-detail-7f2c"),
			status: http.StatusServiceUnavailable,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			service := &fakeAppAdministration{
				createFn: func(
					context.Context,
					AppAdministrationScope,
					AppAdministrationDefinition,
				) (AppAdministrationWorkspace, error) {
					return AppAdministrationWorkspace{}, test.err
				},
			}
			handler := newAppAdministrationTestHandler(
				t,
				service,
				BootstrapConfig{},
			)
			response := postAppAdministrationProto(
				t,
				handler,
				"/api/apps/create",
				&opensplunk.CreateAppRequest{
					Definition: &opensplunk.AppDefinition{
						Slug:        "error",
						DisplayName: "Error",
					},
				},
			)
			if response.Code != test.status {
				t.Fatalf(
					"status = %d, body = %s",
					response.Code,
					response.Body,
				)
			}
			if strings.Contains(
				response.Body.String(),
				"secret-storage-detail",
			) {
				t.Fatalf("error leaked detail: %s", response.Body)
			}
		})
	}
}

func TestAppAdministrationFeaturesTypedNilAndCursorKeyConfiguration(
	t *testing.T,
) {
	t.Parallel()

	service := &fakeAppAdministration{}
	requested := BootstrapConfig{Features: []opensplunk.ServerFeature{
		opensplunk.ServerFeature_SERVER_FEATURE_SEARCH,
		opensplunk.ServerFeature_SERVER_FEATURE_APP_ADMIN,
		opensplunk.ServerFeature_SERVER_FEATURE_APP_ADMIN,
	}}
	enabled := newAppAdministrationTestHandler(t, service, requested)
	bootstrap := postProto(
		t,
		enabled,
		"/api/system/bootstrap",
		&opensplunk.GetSystemBootstrapRequest{},
	)
	var decoded opensplunk.GetSystemBootstrapResponse
	unmarshalResponse(t, bootstrap, &decoded)
	if countFeature(
		decoded.GetFeatures(),
		opensplunk.ServerFeature_SERVER_FEATURE_APP_ADMIN,
	) != 1 {
		t.Fatalf("enabled features = %v", decoded.GetFeatures())
	}

	var typedNil *fakeAppAdministration
	disabled, err := NewHandler(Config{
		SearchJobs:                 &fakeSearchJobs{},
		Indexes:                    fakeIndexCatalog{},
		SavedSearches:              &fakeSavedSearches{},
		AppAdmin:                   typedNil,
		WebUI:                      testUI(),
		Bootstrap:                  requested,
		AdministrativeAllowedHosts: []string{"example.com"},
	})
	if err != nil {
		t.Fatalf("typed-nil AppAdmin: %v", err)
	}
	bootstrap = postProto(
		t,
		disabled,
		"/api/system/bootstrap",
		&opensplunk.GetSystemBootstrapRequest{},
	)
	unmarshalResponse(t, bootstrap, &decoded)
	if countFeature(
		decoded.GetFeatures(),
		opensplunk.ServerFeature_SERVER_FEATURE_APP_ADMIN,
	) != 0 {
		t.Fatalf("disabled features = %v", decoded.GetFeatures())
	}
	route := postAppAdministrationProto(
		t,
		disabled,
		"/api/apps/list",
		&opensplunk.ListAppsRequest{},
	)
	if route.Code != http.StatusNotFound {
		t.Fatalf("disabled route = %d, %s", route.Code, route.Body)
	}

	principal := browserGatePrincipal(
		t,
		browserGateTenantID,
		browserGateOwnerID,
		auth.BrowserRoleAdministrator,
	)
	for name, config := range map[string]Config{
		"auth": {
			AppAdmin:     service,
			AppCursorKey: appAdministrationCursorKey,
		},
		"cursor": {
			AppAdmin: service,
			BrowserAuthenticator: &appAdministrationAuthenticator{
				principal: principal,
			},
		},
		"short cursor": {
			AppAdmin:             service,
			AppCursorKey:         bytes.Repeat([]byte{1}, 31),
			BrowserAuthenticator: &appAdministrationAuthenticator{principal: principal},
		},
	} {
		t.Run(name, func(t *testing.T) {
			config.SearchJobs = &fakeSearchJobs{}
			config.Indexes = fakeIndexCatalog{}
			config.SavedSearches = &fakeSavedSearches{}
			config.WebUI = testUI()
			config.TenantID = browserGateTenantID
			config.OwnerID = browserGateOwnerID
			config.AdministrativeAllowedHosts = []string{"example.com"}
			if _, err := NewHandler(config); err == nil {
				t.Fatal("NewHandler error = nil")
			}
		})
	}
}

func TestAppAdministrationCancellationAndCommittedSuccess(t *testing.T) {
	t.Parallel()

	createContext, cancelCreate := context.WithCancel(context.Background())
	t.Cleanup(cancelCreate)
	getContext, cancelGet := context.WithCancel(context.Background())
	t.Cleanup(cancelGet)
	validDefinition := AppAdministrationDefinition{
		Slug:        "committed",
		DisplayName: "Committed",
	}
	service := &fakeAppAdministration{
		createFn: func(
			ctx context.Context,
			_ AppAdministrationScope,
			_ AppAdministrationDefinition,
		) (AppAdministrationWorkspace, error) {
			cancelCreate()
			<-ctx.Done()
			return appAdministrationFixture(
				"app_0123456789ABCDEFGHIJKL",
				1,
				AppAdministrationStateActive,
				validDefinition,
			), nil
		},
		getFn: func(
			ctx context.Context,
			_ AppAdministrationScope,
			_ AppAdministrationSelector,
		) (AppAdministrationWorkspace, error) {
			cancelGet()
			<-ctx.Done()
			return AppAdministrationWorkspace{}, ctx.Err()
		},
	}
	handler := newAppAdministrationHandlerWithAuthenticator(
		t,
		service,
		&appAdministrationAuthenticator{
			principal: browserGatePrincipal(
				t,
				browserGateTenantID,
				browserGateOwnerID,
				auth.BrowserRoleAdministrator,
			),
		},
	)
	committed := postAppAdministrationProtoContext(
		t,
		createContext,
		handler,
		"/api/apps/create",
		&opensplunk.CreateAppRequest{
			Definition: &opensplunk.AppDefinition{
				Slug:        "committed",
				DisplayName: "Committed",
			},
		},
	)
	if committed.Code != http.StatusOK {
		t.Fatalf(
			"committed success = %d, %s",
			committed.Code,
			committed.Body,
		)
	}
	canceledRead := postAppAdministrationProtoContext(
		t,
		getContext,
		handler,
		"/api/apps/get",
		&opensplunk.GetAppRequest{
			Selector: &opensplunk.AppSelector{
				Selector: &opensplunk.AppSelector_AppId{
					AppId: "app_0123456789ABCDEFGHIJKL",
				},
			},
		},
	)
	if canceledRead.Code != http.StatusRequestTimeout ||
		!strings.Contains(
			canceledRead.Body.String(),
			"app administration request was canceled",
		) {
		t.Fatalf(
			"canceled read = %d, %s",
			canceledRead.Code,
			canceledRead.Body,
		)
	}
	if calls := service.calls(); calls != [6]int{1, 1, 0, 0, 0, 0} {
		t.Fatalf("cancellation service calls = %v", calls)
	}
}

func TestAppAdministrationSerializationCapacityPrecedesMutation(
	t *testing.T,
) {
	t.Parallel()

	entered := make(chan struct{})
	releaseFirst := make(chan struct{})
	service := &fakeAppAdministration{
		createFn: func(
			_ context.Context,
			_ AppAdministrationScope,
			definition AppAdministrationDefinition,
		) (AppAdministrationWorkspace, error) {
			close(entered)
			<-releaseFirst
			return appAdministrationFixture(
				"app_0123456789ABCDEFGHIJKL",
				1,
				AppAdministrationStateActive,
				definition,
			), nil
		},
	}
	principal := browserGatePrincipal(
		t,
		browserGateTenantID,
		browserGateOwnerID,
		auth.BrowserRoleAdministrator,
	)
	handler, err := NewHandler(Config{
		SearchJobs:                 &fakeSearchJobs{},
		Indexes:                    fakeIndexCatalog{},
		SavedSearches:              &fakeSavedSearches{},
		AppAdmin:                   service,
		BrowserAuthenticator:       &appAdministrationAuthenticator{principal: principal},
		AppCursorKey:               appAdministrationCursorKey,
		WebUI:                      testUI(),
		TenantID:                   browserGateTenantID,
		OwnerID:                    browserGateOwnerID,
		MaximumConcurrentResponses: 1,
		AdministrativeAllowedHosts: []string{"example.com"},
	})
	if err != nil {
		t.Fatalf("NewHandler: %v", err)
	}
	firstDone := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		firstDone <- postAppAdministrationProto(
			t,
			handler,
			"/api/apps/create",
			&opensplunk.CreateAppRequest{
				Definition: &opensplunk.AppDefinition{
					Slug:        "first",
					DisplayName: "First",
				},
			},
		)
	}()
	<-entered
	second := postAppAdministrationProto(
		t,
		handler,
		"/api/apps/create",
		&opensplunk.CreateAppRequest{
			Definition: &opensplunk.AppDefinition{
				Slug:        "second",
				DisplayName: "Second",
			},
		},
	)
	if second.Code != http.StatusServiceUnavailable ||
		!strings.Contains(
			second.Body.String(),
			"administrative response capacity is exhausted",
		) ||
		service.calls()[0] != 1 {
		t.Fatalf(
			"second response = %d, %s, calls %v",
			second.Code,
			second.Body,
			service.calls(),
		)
	}
	close(releaseFirst)
	if first := <-firstDone; first.Code != http.StatusOK {
		t.Fatalf("first response = %d, %s", first.Code, first.Body)
	}
}

func TestAppAdministrationDirectHandlerRequiresDetachedPrincipal(
	t *testing.T,
) {
	t.Parallel()

	service := &fakeAppAdministration{}
	handler := &apiHandler{
		appAdmin:          service,
		tenantID:          browserGateTenantID,
		ownerID:           browserGateOwnerID,
		now:               time.Now,
		serializationGate: make(chan struct{}, 1),
	}
	request := httptest.NewRequestWithContext(
		context.Background(),
		http.MethodPost,
		"/api/apps/create",
		nil,
	)
	_, err := handler.createApp(
		request,
		&opensplunk.CreateAppRequest{
			Definition: &opensplunk.AppDefinition{
				Slug:        "direct",
				DisplayName: "Direct",
			},
		},
	)
	if err == nil || service.calls() != ([6]int{}) {
		t.Fatalf("direct call = %v, service %v", err, service.calls())
	}
}

func TestAppAdministrationRejectsMalformedServiceResults(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		mutate func(*AppAdministrationWorkspace)
	}{
		{
			name: "empty ID",
			mutate: func(record *AppAdministrationWorkspace) {
				record.AppID = ""
			},
		},
		{
			name: "zero version",
			mutate: func(record *AppAdministrationWorkspace) {
				record.Version = 0
			},
		},
		{
			name: "unsorted indexes",
			mutate: func(record *AppAdministrationWorkspace) {
				record.Definition.DefaultIndexNames = []string{"z", "a"}
			},
		},
		{
			name: "invalid state",
			mutate: func(record *AppAdministrationWorkspace) {
				record.State = "secret-state"
			},
		},
		{
			name: "timestamp reversal",
			mutate: func(record *AppAdministrationWorkspace) {
				record.UpdatedAt = record.CreatedAt.Add(-time.Second)
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			record := appAdministrationFixture(
				"app_0123456789ABCDEFGHIJKL",
				1,
				AppAdministrationStateActive,
				AppAdministrationDefinition{
					Slug:              "valid",
					DisplayName:       "Valid",
					DefaultIndexNames: []string{"main"},
				},
			)
			test.mutate(&record)
			service := &fakeAppAdministration{
				createFn: func(
					context.Context,
					AppAdministrationScope,
					AppAdministrationDefinition,
				) (AppAdministrationWorkspace, error) {
					return record, nil
				},
			}
			handler := newAppAdministrationTestHandler(
				t,
				service,
				BootstrapConfig{},
			)
			response := postAppAdministrationProto(
				t,
				handler,
				"/api/apps/create",
				&opensplunk.CreateAppRequest{
					Definition: &opensplunk.AppDefinition{
						Slug:        "valid",
						DisplayName: "Valid",
					},
				},
			)
			if response.Code != http.StatusInternalServerError ||
				strings.Contains(response.Body.String(), "secret-state") {
				t.Fatalf(
					"malformed result = %d, %s",
					response.Code,
					response.Body,
				)
			}
		})
	}
}

func appAdministrationFixture(
	id string,
	version uint64,
	state AppAdministrationState,
	definition AppAdministrationDefinition,
) AppAdministrationWorkspace {
	return AppAdministrationWorkspace{
		AppID:      id,
		Version:    version,
		Definition: cloneAppAdministrationDefinition(definition),
		State:      state,
		CreatedAt:  testNow,
		UpdatedAt:  testNow,
	}
}

func newAppAdministrationTestHandler(
	t *testing.T,
	service AppAdministration,
	bootstrap BootstrapConfig,
) *Handler {
	t.Helper()
	return newAppAdministrationHandlerWithAuthenticator(
		t,
		service,
		&appAdministrationAuthenticator{
			principal: browserGatePrincipal(
				t,
				browserGateTenantID,
				browserGateOwnerID,
				auth.BrowserRoleAdministrator,
			),
		},
		bootstrap,
	)
}

func newAppAdministrationHandlerWithAuthenticator(
	t *testing.T,
	service AppAdministration,
	authenticator auth.BrowserAuthenticator,
	bootstrap ...BootstrapConfig,
) *Handler {
	t.Helper()
	var configuredBootstrap BootstrapConfig
	if len(bootstrap) != 0 {
		configuredBootstrap = bootstrap[0]
	}
	handler, err := NewHandler(Config{
		SearchJobs:                 &fakeSearchJobs{},
		Indexes:                    fakeIndexCatalog{},
		SavedSearches:              &fakeSavedSearches{},
		AppAdmin:                   service,
		BrowserAuthenticator:       authenticator,
		AppCursorKey:               slices.Clone(appAdministrationCursorKey),
		WebUI:                      testUI(),
		Bootstrap:                  configuredBootstrap,
		TenantID:                   browserGateTenantID,
		OwnerID:                    browserGateOwnerID,
		RouteTimeout:               5 * time.Second,
		AdministrativeAllowedHosts: []string{"example.com"},
	})
	if err != nil {
		t.Fatalf("NewHandler: %v", err)
	}
	return handler
}

func newAppAdministrationHandlerWithKey(
	t *testing.T,
	service AppAdministration,
	key []byte,
) *Handler {
	t.Helper()
	handler, err := NewHandler(Config{
		SearchJobs:    &fakeSearchJobs{},
		Indexes:       fakeIndexCatalog{},
		SavedSearches: &fakeSavedSearches{},
		AppAdmin:      service,
		BrowserAuthenticator: &appAdministrationAuthenticator{
			principal: browserGatePrincipal(
				t,
				browserGateTenantID,
				browserGateOwnerID,
				auth.BrowserRoleAdministrator,
			),
		},
		AppCursorKey:               key,
		WebUI:                      testUI(),
		TenantID:                   browserGateTenantID,
		OwnerID:                    browserGateOwnerID,
		AdministrativeAllowedHosts: []string{"example.com"},
	})
	if err != nil {
		t.Fatalf("NewHandler: %v", err)
	}
	return handler
}

func postAppAdministrationProto(
	t *testing.T,
	handler http.Handler,
	path string,
	message proto.Message,
) *httptest.ResponseRecorder {
	t.Helper()
	return postAppAdministrationProtoContext(
		t,
		context.Background(),
		handler,
		path,
		message,
	)
}

func postAppAdministrationProtoContext(
	t *testing.T,
	ctx context.Context,
	handler http.Handler,
	path string,
	message proto.Message,
) *httptest.ResponseRecorder {
	t.Helper()
	payload, err := proto.Marshal(message)
	if err != nil {
		t.Fatalf("marshal app request: %v", err)
	}
	request := httptest.NewRequestWithContext(
		ctx,
		http.MethodPost,
		path,
		bytes.NewReader(payload),
	)
	request.Host = "example.com"
	request.Header.Set("Content-Type", "application/x-protobuf")
	request.Header.Set(
		"Authorization",
		"Bearer "+appAdministrationBearerToken,
	)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}
