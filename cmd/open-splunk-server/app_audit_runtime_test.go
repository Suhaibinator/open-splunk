package main

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"path/filepath"
	"testing"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/audit"
	"github.com/Suhaibinator/open-splunk/internal/auth"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/server"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

func TestRuntimeAppCatalogAuditsEveryMutationWithAuthenticatedActor(t *testing.T) {
	ctx := context.Background()
	directory := t.TempDir()
	database, err := control.Open(ctx, filepath.Join(directory, "control.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if closeErr := database.Close(); closeErr != nil {
			t.Error(closeErr)
		}
	})

	auditStore, err := audit.NewStore(database, audit.StoreOptions{
		CursorKey: bytes.Repeat([]byte("a"), 32),
	})
	if err != nil {
		t.Fatal(err)
	}
	keyPath := filepath.Join(directory, "server.key")
	if catalog, key, constructorErr := newRuntimeAuditedAppCatalog(
		ctx,
		database,
		keyPath,
		nil,
	); constructorErr == nil || catalog != nil || key != nil {
		t.Fatalf(
			"audited app constructor without appender = %#v/%x/%v",
			catalog,
			key,
			constructorErr,
		)
	}

	catalog, administrationKey, err := newRuntimeAuditedAppCatalog(
		ctx,
		database,
		keyPath,
		auditStore,
	)
	if err != nil {
		t.Fatal(err)
	}
	mismatchedContext, err := audit.WithActor(ctx, audit.Actor{
		Kind: audit.ActorKindBrowser,
		ID:   "different-administrator",
		Role: audit.ActorRoleAdministrator,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, mismatchErr := catalog.CreateApp(
		mismatchedContext,
		server.AppAdministrationScope{
			TenantID: "tenant-a",
			ActorID:  "administrator",
		},
		server.AppAdministrationDefinition{Slug: "wrong-attribution"},
	); !errors.Is(mismatchErr, server.ErrAppAdministrationInvalidArgument) {
		t.Fatalf("mismatched app audit actor error = %v", mismatchErr)
	}

	bearerToken := bytes.Repeat([]byte("b"), auth.MinimumBrowserBearerTokenBytes)
	authenticator, err := auth.NewBearerTokenAuthenticator(
		bearerToken,
		"tenant-a",
		"administrator",
		auth.BrowserRoleAdministrator,
	)
	if err != nil {
		t.Fatal(err)
	}
	handler := newRuntimeAppHTTPHandlerForTest(
		t,
		catalog,
		administrationKey,
		authenticator,
		auditStore,
	)
	t.Cleanup(func() {
		if closeErr := handler.Close(context.Background()); closeErr != nil {
			t.Error(closeErr)
		}
	})
	response := postRuntimeAppProto(
		t,
		handler,
		"/api/v1/apps/create",
		&opensplunkv1.CreateAppRequest{
			Definition: &opensplunkv1.AppDefinition{
				Slug:        "audited-app",
				DisplayName: "Audited App",
			},
		},
		bearerToken,
	)
	if response.Code != http.StatusOK {
		t.Fatalf(
			"create audited app status = %d, body = %s",
			response.Code,
			response.Body.String(),
		)
	}
	var createdResponse opensplunkv1.CreateAppResponse
	unmarshalRuntimeAppResponse(t, response, &createdResponse)
	created := createdResponse.GetApp()
	if created == nil || created.GetVersion() != 1 {
		t.Fatalf("created audited app = %#v", created)
	}

	selector := &opensplunkv1.AppSelector{
		Selector: &opensplunkv1.AppSelector_AppId{AppId: created.GetAppId()},
	}
	response = postRuntimeAppProto(
		t,
		handler,
		"/api/v1/apps/update",
		&opensplunkv1.UpdateAppRequest{
			Selector:        selector,
			ExpectedVersion: created.GetVersion(),
			Definition: &opensplunkv1.AppDefinition{
				DisplayName: "Updated Audited App",
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"display_name"}},
		},
		bearerToken,
	)
	if response.Code != http.StatusOK {
		t.Fatalf("update status = %d, body = %s", response.Code, response.Body)
	}
	var updatedResponse opensplunkv1.UpdateAppResponse
	unmarshalRuntimeAppResponse(t, response, &updatedResponse)
	updated := updatedResponse.GetApp()
	if updated.GetVersion() != 2 ||
		updated.GetDefinition().GetDisplayName() != "Updated Audited App" {
		t.Fatalf("updated audited app = %#v", updated)
	}

	setState := func(
		current *opensplunkv1.AppWorkspace,
		state opensplunkv1.AppState,
	) *opensplunkv1.AppWorkspace {
		t.Helper()
		stateResponse := postRuntimeAppProto(
			t,
			handler,
			"/api/v1/apps/state/set",
			&opensplunkv1.SetAppStateRequest{
				Selector:        selector,
				ExpectedVersion: current.GetVersion(),
				State:           state,
			},
			bearerToken,
		)
		if stateResponse.Code != http.StatusOK {
			t.Fatalf(
				"state %s status = %d, body = %s",
				state,
				stateResponse.Code,
				stateResponse.Body,
			)
		}
		var decoded opensplunkv1.SetAppStateResponse
		unmarshalRuntimeAppResponse(t, stateResponse, &decoded)
		if decoded.GetApp().GetVersion() != current.GetVersion()+1 ||
			decoded.GetApp().GetState() != state {
			t.Fatalf("state-updated audited app = %#v", decoded.GetApp())
		}
		return decoded.GetApp()
	}
	archived := setState(
		updated,
		opensplunkv1.AppState_APP_STATE_ARCHIVED,
	)
	active := setState(
		archived,
		opensplunkv1.AppState_APP_STATE_ACTIVE,
	)
	finalArchived := setState(
		active,
		opensplunkv1.AppState_APP_STATE_ARCHIVED,
	)

	response = postRuntimeAppProto(
		t,
		handler,
		"/api/v1/apps/delete",
		&opensplunkv1.DeleteAppRequest{
			Selector:         selector,
			ExpectedVersion:  finalArchived.GetVersion(),
			ConfirmationSlug: "audited-app",
		},
		bearerToken,
	)
	if response.Code != http.StatusOK {
		t.Fatalf("delete status = %d, body = %s", response.Code, response.Body)
	}
	var deletedResponse opensplunkv1.DeleteAppResponse
	unmarshalRuntimeAppResponse(t, response, &deletedResponse)
	if deletedResponse.GetAppId() != created.GetAppId() {
		t.Fatalf(
			"deleted app ID = %q, want %q",
			deletedResponse.GetAppId(),
			created.GetAppId(),
		)
	}

	response = postRuntimeAppProto(
		t,
		handler,
		"/api/v1/apps/get",
		&opensplunkv1.GetAppRequest{Selector: selector},
		bearerToken,
	)
	if response.Code != http.StatusNotFound {
		t.Fatalf("get deleted app = %d, %s", response.Code, response.Body)
	}

	pageSize := uint32(20)
	targetKind := opensplunkv1.AuditTargetKind_AUDIT_TARGET_KIND_APP
	response = postRuntimeAppProto(
		t,
		handler,
		"/api/v1/audit/events/list",
		&opensplunkv1.ListAuditEventsRequest{
			Page: &opensplunkv1.PageRequest{
				PageSize:         &pageSize,
				IncludeTotalSize: true,
			},
			TargetKindFilter: &targetKind,
		},
		bearerToken,
	)
	if response.Code != http.StatusOK {
		t.Fatalf("list app audit = %d, %s", response.Code, response.Body)
	}
	var page opensplunkv1.ListAuditEventsResponse
	unmarshalRuntimeAppResponse(t, response, &page)
	wantActions := []opensplunkv1.AuditAction{
		opensplunkv1.AuditAction_AUDIT_ACTION_APP_DELETE,
		opensplunkv1.AuditAction_AUDIT_ACTION_APP_ARCHIVE,
		opensplunkv1.AuditAction_AUDIT_ACTION_APP_ACTIVATE,
		opensplunkv1.AuditAction_AUDIT_ACTION_APP_ARCHIVE,
		opensplunkv1.AuditAction_AUDIT_ACTION_APP_UPDATE,
		opensplunkv1.AuditAction_AUDIT_ACTION_APP_CREATE,
	}
	wantVersions := []uint64{5, 5, 4, 3, 2, 1}
	if len(page.GetAuditEvents()) != len(wantActions) ||
		page.GetPage().GetTotalSize() != uint64(len(wantActions)) ||
		!page.GetPage().GetTotalSizeExact() ||
		page.GetPage().GetNextPageToken() != "" {
		t.Fatalf("app audit page = %#v", &page)
	}
	for index, event := range page.GetAuditEvents() {
		if event.GetAction() != wantActions[index] ||
			event.GetTargetVersion() != wantVersions[index] ||
			event.GetTargetKind() != targetKind ||
			event.GetTargetId() != created.GetAppId() ||
			event.GetActorKind() !=
				opensplunkv1.AuditActorKind_AUDIT_ACTOR_KIND_BROWSER ||
			event.GetActorId() != "administrator" ||
			event.GetActorRole() !=
				opensplunkv1.AuditActorRole_AUDIT_ACTOR_ROLE_ADMINISTRATOR {
			t.Fatalf(
				"app audit event %d = %#v, want action/version %s/%d",
				index,
				event,
				wantActions[index],
				wantVersions[index],
			)
		}
	}
}
