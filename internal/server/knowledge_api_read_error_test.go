package server

import (
	"context"
	"errors"
	"net/http"
	"testing"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/auth"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/knowledgeattemptaudit"
	"github.com/Suhaibinator/open-splunk/internal/knowledgecatalog"
	"google.golang.org/protobuf/proto"
)

func TestKnowledgeHTTPReadRoutesCollapseImpossibleCatalogSentinels(
	t *testing.T,
) {
	t.Parallel()

	object := knowledgeHTTPObject()
	objectContext := knowledgecatalog.AuthorizedContext{
		AppID: knowledgeHTTPAppID,
		Object: &knowledgecatalog.AuthorizedObject{
			KnowledgeObjectID: object.KnowledgeObjectID,
			ObjectType:        object.ObjectType,
			Version:           object.Version,
			SharingScope:      object.SharingScope,
		},
	}
	appContext := knowledgecatalog.AuthorizedContext{AppID: knowledgeHTTPAppID}
	tests := []struct {
		name       string
		path       string
		request    proto.Message
		cause      error
		authorized knowledgecatalog.AuthorizedContext
	}{
		{name: "get invalid argument", path: knowledgeObjectsGetPath, request: &opensplunkv1.GetKnowledgeObjectRequest{KnowledgeObjectId: object.KnowledgeObjectID}, cause: control.ErrInvalidArgument, authorized: objectContext},
		{name: "get invalid cursor", path: knowledgeObjectsGetPath, request: &opensplunkv1.GetKnowledgeObjectRequest{KnowledgeObjectId: object.KnowledgeObjectID}, cause: knowledgecatalog.ErrInvalidCursor, authorized: objectContext},
		{name: "get page invalidated", path: knowledgeObjectsGetPath, request: &opensplunkv1.GetKnowledgeObjectRequest{KnowledgeObjectId: object.KnowledgeObjectID}, cause: control.ErrPageInvalidated, authorized: objectContext},
		{name: "get version conflict", path: knowledgeObjectsGetPath, request: &opensplunkv1.GetKnowledgeObjectRequest{KnowledgeObjectId: object.KnowledgeObjectID}, cause: control.ErrVersionConflict, authorized: objectContext},
		{name: "get capacity", path: knowledgeObjectsGetPath, request: &opensplunkv1.GetKnowledgeObjectRequest{KnowledgeObjectId: object.KnowledgeObjectID}, cause: control.ErrCapacityExceeded, authorized: objectContext},
		{name: "list invalid argument", path: knowledgeObjectsListPath, request: &opensplunkv1.ListKnowledgeObjectsRequest{}, cause: control.ErrInvalidArgument, authorized: appContext},
		{name: "list not found", path: knowledgeObjectsListPath, request: &opensplunkv1.ListKnowledgeObjectsRequest{}, cause: control.ErrNotFound, authorized: appContext},
		{name: "list version conflict", path: knowledgeObjectsListPath, request: &opensplunkv1.ListKnowledgeObjectsRequest{}, cause: control.ErrVersionConflict, authorized: appContext},
		{name: "list dependency conflict", path: knowledgeObjectsListPath, request: &opensplunkv1.ListKnowledgeObjectsRequest{}, cause: control.ErrDependencyConflict, authorized: appContext},
		{name: "list capacity", path: knowledgeObjectsListPath, request: &opensplunkv1.ListKnowledgeObjectsRequest{}, cause: control.ErrCapacityExceeded, authorized: appContext},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			serviceErr := knowledgecatalog.WithAuthorizedContext(
				test.cause,
				test.authorized,
			)
			catalog := &knowledgeHTTPCatalog{
				getFn: func(
					context.Context,
					knowledgecatalog.ReadScope,
					string,
					*uint64,
				) (knowledgecatalog.Object, error) {
					return knowledgecatalog.Object{}, serviceErr
				},
				listFn: func(
					context.Context,
					knowledgecatalog.ReadScope,
					knowledgecatalog.ListRequest,
				) (knowledgecatalog.ListPage, error) {
					return knowledgecatalog.ListPage{}, serviceErr
				},
			}
			appender := &knowledgeBoundaryAppender{}
			_, httpHandler := newKnowledgeHTTPHandler(
				t,
				auth.BrowserRoleAdministrator,
				catalog,
				&knowledgeHTTPWriter{},
				knowledgeHTTPApps(),
				appender,
			)
			response := knowledgeHTTPPost(
				t,
				httpHandler,
				test.path,
				test.request,
			)
			attempts := appender.snapshot()
			if response.Code != http.StatusServiceUnavailable ||
				response.Body.String() != knowledgeManagementUnavailableBody ||
				len(attempts) != 1 ||
				attempts[0].definition.Reason != knowledgeattemptaudit.ReasonServiceUnavailable ||
				attempts[0].definition.AuthorizedContext != nil {
				t.Fatalf(
					"status=%d body=%q attempts=%+v",
					response.Code,
					response.Body.String(),
					attempts,
				)
			}
		})
	}
}

func TestKnowledgeHTTPReadRoutesRetainOnlyTheirSupportedErrorFamilies(
	t *testing.T,
) {
	t.Parallel()

	tests := []struct {
		name       string
		path       string
		request    proto.Message
		serviceErr error
		wantStatus int
		wantReason knowledgeattemptaudit.Reason
	}{
		{
			name:       "get not found",
			path:       knowledgeObjectsGetPath,
			request:    &opensplunkv1.GetKnowledgeObjectRequest{KnowledgeObjectId: "ko-http-object-1"},
			serviceErr: control.ErrNotFound,
			wantStatus: http.StatusNotFound,
			wantReason: knowledgeattemptaudit.ReasonNotFoundOrForbidden,
		},
		{
			name:       "get cancellation",
			path:       knowledgeObjectsGetPath,
			request:    &opensplunkv1.GetKnowledgeObjectRequest{KnowledgeObjectId: "ko-http-object-1"},
			serviceErr: context.Canceled,
			wantStatus: http.StatusRequestTimeout,
			wantReason: knowledgeattemptaudit.ReasonServiceUnavailable,
		},
		{
			name:       "list invalid cursor",
			path:       knowledgeObjectsListPath,
			request:    &opensplunkv1.ListKnowledgeObjectsRequest{},
			serviceErr: knowledgecatalog.ErrInvalidCursor,
			wantStatus: http.StatusBadRequest,
			wantReason: knowledgeattemptaudit.ReasonInvalidDefinition,
		},
		{
			name:       "list page invalidated",
			path:       knowledgeObjectsListPath,
			request:    &opensplunkv1.ListKnowledgeObjectsRequest{},
			serviceErr: control.ErrPageInvalidated,
			wantStatus: http.StatusConflict,
			wantReason: knowledgeattemptaudit.ReasonServiceUnavailable,
		},
		{
			name:       "list cancellation",
			path:       knowledgeObjectsListPath,
			request:    &opensplunkv1.ListKnowledgeObjectsRequest{},
			serviceErr: context.Canceled,
			wantStatus: http.StatusRequestTimeout,
			wantReason: knowledgeattemptaudit.ReasonServiceUnavailable,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			catalog := &knowledgeHTTPCatalog{
				getFn: func(
					context.Context,
					knowledgecatalog.ReadScope,
					string,
					*uint64,
				) (knowledgecatalog.Object, error) {
					return knowledgecatalog.Object{}, test.serviceErr
				},
				listFn: func(
					context.Context,
					knowledgecatalog.ReadScope,
					knowledgecatalog.ListRequest,
				) (knowledgecatalog.ListPage, error) {
					return knowledgecatalog.ListPage{}, test.serviceErr
				},
			}
			appender := &knowledgeBoundaryAppender{}
			_, httpHandler := newKnowledgeHTTPHandler(
				t,
				auth.BrowserRoleAdministrator,
				catalog,
				&knowledgeHTTPWriter{},
				knowledgeHTTPApps(),
				appender,
			)
			response := knowledgeHTTPPost(
				t,
				httpHandler,
				test.path,
				test.request,
			)
			attempts := appender.snapshot()
			if response.Code != test.wantStatus || len(attempts) != 1 ||
				attempts[0].definition.Reason != test.wantReason ||
				attempts[0].definition.AuthorizedContext != nil {
				t.Fatalf(
					"status=%d body=%q attempts=%+v",
					response.Code,
					response.Body.String(),
					attempts,
				)
			}
		})
	}
}

func TestKnowledgeHTTPReadErrorSanitizersRejectJoinedImpossibleSentinel(t *testing.T) {
	t.Parallel()

	joined := errors.Join(context.Canceled, control.ErrInvalidArgument)
	if got := sanitizeKnowledgeGetError(joined); errors.Is(got, context.Canceled) ||
		errors.Is(got, control.ErrInvalidArgument) {
		t.Fatalf("Get sanitizer retained joined sentinel: %v", got)
	}
	if got := sanitizeKnowledgeListError(joined); errors.Is(got, context.Canceled) ||
		errors.Is(got, control.ErrInvalidArgument) {
		t.Fatalf("List sanitizer retained joined sentinel: %v", got)
	}
}
