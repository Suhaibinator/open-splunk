package server

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Suhaibinator/SRouter/pkg/router"
	"github.com/Suhaibinator/open-splunk/internal/audit"
	"github.com/Suhaibinator/open-splunk/internal/auth"
	"github.com/Suhaibinator/open-splunk/internal/knowledgeattemptaudit"
)

const (
	knowledgeBoundaryToken    = "0123456789abcdefghijklmnopqrstuv"
	knowledgeBoundaryTenantID = "tenant-knowledge-boundary"
	knowledgeBoundaryOwnerID  = "owner-knowledge-boundary"
)

var knowledgeBoundaryNow = time.Date(
	2026,
	time.August,
	7,
	18,
	19,
	20,
	123456789,
	time.FixedZone("fixture", -7*60*60),
)

type knowledgeBoundaryAppendCall struct {
	tenantID   string
	definition knowledgeattemptaudit.Definition
	contextErr error
	actor      audit.Actor
	actorOK    bool
	deadline   time.Time
	deadlineOK bool
}

type knowledgeBoundaryAppender struct {
	mu    sync.Mutex
	calls []knowledgeBoundaryAppendCall
	err   error
	hook  func(context.Context, string, knowledgeattemptaudit.Definition)
}

func (appender *knowledgeBoundaryAppender) AppendRejected(
	ctx context.Context,
	tenantID string,
	definition knowledgeattemptaudit.Definition,
) error {
	if appender.hook != nil {
		appender.hook(ctx, tenantID, definition)
	}
	actor, actorOK := audit.ActorFromContext(ctx)
	deadline, deadlineOK := ctx.Deadline()
	call := knowledgeBoundaryAppendCall{
		tenantID: tenantID,
		definition: knowledgeattemptaudit.Definition{
			OccurredAt: definition.OccurredAt,
			Action:     definition.Action,
			Reason:     definition.Reason,
			AuthorizedContext: cloneKnowledgeAttemptAuthorizedContext(
				definition.AuthorizedContext,
			),
		},
		contextErr: ctx.Err(),
		actor:      actor,
		actorOK:    actorOK,
		deadline:   deadline,
		deadlineOK: deadlineOK,
	}
	appender.mu.Lock()
	appender.calls = append(appender.calls, call)
	err := appender.err
	appender.mu.Unlock()
	return err
}

func (appender *knowledgeBoundaryAppender) snapshot() []knowledgeBoundaryAppendCall {
	appender.mu.Lock()
	defer appender.mu.Unlock()
	result := make([]knowledgeBoundaryAppendCall, len(appender.calls))
	copy(result, appender.calls)
	return result
}

func (appender *knowledgeBoundaryAppender) setError(err error) {
	appender.mu.Lock()
	appender.err = err
	appender.mu.Unlock()
}

type knowledgeBoundaryAuthenticator struct {
	mu        sync.Mutex
	principal auth.BrowserPrincipal
	calls     int
	hook      func()
}

func (authenticator *knowledgeBoundaryAuthenticator) Authenticate(
	ctx context.Context,
	_ []byte,
) (auth.BrowserPrincipal, error) {
	if err := ctx.Err(); err != nil {
		return auth.BrowserPrincipal{}, err
	}
	if authenticator.hook != nil {
		authenticator.hook()
	}
	authenticator.mu.Lock()
	authenticator.calls++
	principal := authenticator.principal
	authenticator.mu.Unlock()
	return principal, nil
}

func (authenticator *knowledgeBoundaryAuthenticator) callCount() int {
	authenticator.mu.Lock()
	defer authenticator.mu.Unlock()
	return authenticator.calls
}

type knowledgeBoundaryObservedBody struct {
	mu        sync.Mutex
	reader    *strings.Reader
	readCalls int
	observed  bool
	onRead    func()
}

func newKnowledgeBoundaryObservedBody(
	value string,
	onRead func(),
) *knowledgeBoundaryObservedBody {
	return &knowledgeBoundaryObservedBody{
		reader: strings.NewReader(value),
		onRead: onRead,
	}
}

func (body *knowledgeBoundaryObservedBody) Read(payload []byte) (int, error) {
	body.mu.Lock()
	body.readCalls++
	if !body.observed {
		body.observed = true
		if body.onRead != nil {
			body.onRead()
		}
	}
	result, err := body.reader.Read(payload)
	body.mu.Unlock()
	return result, err
}

func (*knowledgeBoundaryObservedBody) Close() error { return nil }

func (body *knowledgeBoundaryObservedBody) reads() int {
	body.mu.Lock()
	defer body.mu.Unlock()
	return body.readCalls
}

func knowledgeBoundaryPrincipal(
	t *testing.T,
	role auth.BrowserRole,
) auth.BrowserPrincipal {
	t.Helper()
	authenticator, err := auth.NewBearerTokenAuthenticator(
		[]byte(knowledgeBoundaryToken),
		knowledgeBoundaryTenantID,
		knowledgeBoundaryOwnerID,
		role,
	)
	if err != nil {
		t.Fatalf("NewBearerTokenAuthenticator: %v", err)
	}
	principal, err := authenticator.Authenticate(
		context.Background(),
		[]byte(knowledgeBoundaryToken),
	)
	if err != nil {
		t.Fatalf("Authenticate test principal: %v", err)
	}
	return principal
}

func newKnowledgeBoundaryHandler(
	t *testing.T,
	role auth.BrowserRole,
	appender *knowledgeBoundaryAppender,
) *apiHandler {
	t.Helper()
	return &apiHandler{
		browserAuthenticator: &knowledgeBoundaryAuthenticator{
			principal: knowledgeBoundaryPrincipal(t, role),
		},
		knowledgeAttempts:    appender,
		tenantID:             knowledgeBoundaryTenantID,
		ownerID:              knowledgeBoundaryOwnerID,
		routeTimeout:         time.Second,
		knowledgeAttemptGate: make(chan struct{}, 8),
		now: func() time.Time {
			return knowledgeBoundaryNow
		},
	}
}

func knowledgeBoundaryRequest(
	ctx context.Context,
	path string,
	body io.Reader,
) *http.Request {
	request := httptest.NewRequestWithContext(ctx, http.MethodPost, path, body)
	request.Header.Set("Authorization", "Bearer "+knowledgeBoundaryToken)
	request.Header.Set("Content-Type", "application/x-protobuf")
	return request
}

func writeKnowledgeBoundaryHTTPError(
	t *testing.T,
	response http.ResponseWriter,
	err error,
) {
	t.Helper()
	var httpError *router.HTTPError
	if !errors.As(err, &httpError) {
		t.Fatalf("error = %T %v, want router.HTTPError", err, err)
	}
	writeAPIError(response, httpError.StatusCode, httpError.Message)
}

func TestKnowledgeAttemptBoundaryRecognizesOnlyExactPostRoutes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		path   string
		action knowledgeattemptaudit.Action
	}{
		{path: "/api/v1/knowledge/objects/create", action: knowledgeattemptaudit.ActionCreate},
		{path: "/api/v1/knowledge/objects/get", action: knowledgeattemptaudit.ActionGet},
		{path: "/api/v1/knowledge/objects/list", action: knowledgeattemptaudit.ActionList},
		{path: "/api/v1/knowledge/objects/dependencies", action: knowledgeattemptaudit.ActionDependencies},
		{path: "/api/v1/knowledge/objects/dependents", action: knowledgeattemptaudit.ActionDependents},
		{path: "/api/v1/knowledge/objects/validate", action: knowledgeattemptaudit.ActionValidate},
		{path: "/api/v1/knowledge/objects/update", action: knowledgeattemptaudit.ActionUpdate},
		{path: "/api/v1/knowledge/objects/set-state", action: knowledgeattemptaudit.ActionUpdate},
		{path: "/api/v1/knowledge/objects/delete", action: knowledgeattemptaudit.ActionDelete},
	}
	for _, test := range tests {
		t.Run(test.path, func(t *testing.T) {
			t.Parallel()
			appender := &knowledgeBoundaryAppender{}
			handler := newKnowledgeBoundaryHandler(
				t,
				auth.BrowserRoleUser,
				appender,
			)
			body := newKnowledgeBoundaryObservedBody("secret request", nil)
			response := httptest.NewRecorder()
			called := false
			handler.protectKnowledgeManagementRoutes(http.HandlerFunc(
				func(http.ResponseWriter, *http.Request) {
					called = true
				},
			)).ServeHTTP(response, knowledgeBoundaryRequest(
				context.Background(),
				test.path,
				body,
			))

			calls := appender.snapshot()
			if called || body.reads() != 0 || response.Code != http.StatusForbidden ||
				response.Body.String() != knowledgeAdministratorRequiredBody ||
				len(calls) != 1 || calls[0].tenantID != knowledgeBoundaryTenantID ||
				calls[0].definition.Action != test.action ||
				calls[0].definition.Reason != knowledgeattemptaudit.ReasonNotAdministrator ||
				calls[0].definition.AuthorizedContext != nil {
				t.Fatalf(
					"called=%v reads=%d status=%d body=%q calls=%+v",
					called,
					body.reads(),
					response.Code,
					response.Body.String(),
					calls,
				)
			}
		})
	}

	for _, test := range []struct {
		name   string
		method string
		path   string
	}{
		{name: "wrong method", method: http.MethodGet, path: tests[0].path},
		{name: "wrong path", method: http.MethodPost, path: tests[0].path + "/typo"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			appender := &knowledgeBoundaryAppender{}
			authenticator := &knowledgeBoundaryAuthenticator{
				principal: knowledgeBoundaryPrincipal(t, auth.BrowserRoleAdministrator),
			}
			handler := &apiHandler{
				browserAuthenticator: authenticator,
				knowledgeAttempts:    appender,
				tenantID:             knowledgeBoundaryTenantID,
				ownerID:              knowledgeBoundaryOwnerID,
				routeTimeout:         time.Second,
				knowledgeAttemptGate: make(chan struct{}, 1),
				now:                  func() time.Time { return knowledgeBoundaryNow },
			}
			response := httptest.NewRecorder()
			handler.protectKnowledgeManagementRoutes(http.HandlerFunc(
				func(response http.ResponseWriter, _ *http.Request) {
					response.WriteHeader(http.StatusNoContent)
				},
			)).ServeHTTP(response, httptest.NewRequestWithContext(t.Context(), test.method, test.path, nil))
			if response.Code != http.StatusNoContent ||
				authenticator.callCount() != 0 || len(appender.snapshot()) != 0 {
				t.Fatalf(
					"status=%d auth calls=%d append calls=%d",
					response.Code,
					authenticator.callCount(),
					len(appender.snapshot()),
				)
			}
		})
	}
}

func TestKnowledgeAttemptBoundaryAuthenticatesBeforeReadingBody(t *testing.T) {
	t.Parallel()

	var mu sync.Mutex
	var order []string
	record := func(value string) {
		mu.Lock()
		order = append(order, value)
		mu.Unlock()
	}
	appender := &knowledgeBoundaryAppender{
		hook: func(context.Context, string, knowledgeattemptaudit.Definition) {
			record("append")
		},
	}
	handler := &apiHandler{
		browserAuthenticator: &knowledgeBoundaryAuthenticator{
			principal: knowledgeBoundaryPrincipal(t, auth.BrowserRoleAdministrator),
			hook:      func() { record("authenticate") },
		},
		knowledgeAttempts:    appender,
		tenantID:             knowledgeBoundaryTenantID,
		ownerID:              knowledgeBoundaryOwnerID,
		routeTimeout:         time.Second,
		knowledgeAttemptGate: make(chan struct{}, 1),
		now:                  func() time.Time { return knowledgeBoundaryNow },
	}
	body := newKnowledgeBoundaryObservedBody("request body", func() {
		record("body")
	})
	response := httptest.NewRecorder()
	handler.protectKnowledgeManagementRoutes(http.HandlerFunc(
		func(response http.ResponseWriter, request *http.Request) {
			_, _ = io.ReadAll(request.Body)
			response.WriteHeader(http.StatusBadRequest)
		},
	)).ServeHTTP(response, knowledgeBoundaryRequest(
		context.Background(),
		"/api/v1/knowledge/objects/create",
		body,
	))

	mu.Lock()
	observedOrder := append([]string(nil), order...)
	mu.Unlock()
	if strings.Join(observedOrder, ",") != "authenticate,body,append" ||
		response.Code != http.StatusBadRequest {
		t.Fatalf("order=%v status=%d", observedOrder, response.Code)
	}
}

func TestKnowledgeAttemptBoundaryUserAppendPrecedesFixedForbiddenAndLeavesBodyUnread(t *testing.T) {
	t.Parallel()

	response := httptest.NewRecorder()
	appender := &knowledgeBoundaryAppender{
		hook: func(context.Context, string, knowledgeattemptaudit.Definition) {
			if response.Code != http.StatusOK || response.Body.Len() != 0 {
				t.Fatalf(
					"response became visible before append: status=%d body=%q",
					response.Code,
					response.Body.String(),
				)
			}
		},
	}
	body := newKnowledgeBoundaryObservedBody("must remain unread", nil)
	handler := newKnowledgeBoundaryHandler(t, auth.BrowserRoleUser, appender)
	handler.protectKnowledgeManagementRoutes(http.HandlerFunc(
		func(http.ResponseWriter, *http.Request) {
			t.Fatal("ordinary user reached knowledge handler")
		},
	)).ServeHTTP(response, knowledgeBoundaryRequest(
		context.Background(),
		"/api/v1/knowledge/objects/get",
		body,
	))

	if body.reads() != 0 || response.Code != http.StatusForbidden ||
		response.Body.String() != knowledgeAdministratorRequiredBody ||
		response.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf(
			"reads=%d status=%d headers=%v body=%q",
			body.reads(),
			response.Code,
			response.Header(),
			response.Body.String(),
		)
	}
}

func TestKnowledgeAttemptBoundaryBoundsDetachedJournalTailsAndReturnsPermit(
	t *testing.T,
) {
	t.Parallel()

	started := make(chan struct{})
	release := make(chan struct{})
	var hookCalls atomic.Int32
	appender := &knowledgeBoundaryAppender{
		hook: func(context.Context, string, knowledgeattemptaudit.Definition) {
			if hookCalls.Add(1) == 1 {
				close(started)
				<-release
			}
		},
	}
	gate := make(chan struct{}, 1)
	user := newKnowledgeBoundaryHandler(t, auth.BrowserRoleUser, appender)
	user.knowledgeAttemptGate = gate
	administrator := newKnowledgeBoundaryHandler(
		t,
		auth.BrowserRoleAdministrator,
		appender,
	)
	administrator.knowledgeAttemptGate = gate

	userBoundary := user.protectKnowledgeManagementRoutes(http.HandlerFunc(
		func(response http.ResponseWriter, _ *http.Request) {
			response.WriteHeader(http.StatusTeapot)
		},
	))
	administratorBoundary := administrator.protectKnowledgeManagementRoutes(
		http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
			response.WriteHeader(http.StatusBadRequest)
		}),
	)
	firstDone := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		response := httptest.NewRecorder()
		userBoundary.ServeHTTP(
			response,
			knowledgeBoundaryRequest(
				context.Background(),
				knowledgeObjectsGetPath,
				newKnowledgeBoundaryObservedBody("first unread body", nil),
			),
		)
		firstDone <- response
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first rejected-attempt append did not acquire its permit")
	}

	serveBounded := func(
		boundary http.Handler,
		path string,
		body *knowledgeBoundaryObservedBody,
	) *httptest.ResponseRecorder {
		t.Helper()
		done := make(chan *httptest.ResponseRecorder, 1)
		go func() {
			response := httptest.NewRecorder()
			boundary.ServeHTTP(
				response,
				knowledgeBoundaryRequest(
					context.Background(),
					path,
					body,
				),
			)
			done <- response
		}()
		select {
		case response := <-done:
			return response
		case <-time.After(500 * time.Millisecond):
			t.Fatal("saturated rejected-attempt gate did not fail immediately")
			return nil
		}
	}

	secondUserBody := newKnowledgeBoundaryObservedBody("second unread body", nil)
	secondUser := serveBounded(
		userBoundary,
		knowledgeObjectsGetPath,
		secondUserBody,
	)
	administratorBody := newKnowledgeBoundaryObservedBody("admin unread body", nil)
	secondAdministrator := serveBounded(
		administratorBoundary,
		knowledgeObjectsCreatePath,
		administratorBody,
	)
	if secondUser.Code != http.StatusServiceUnavailable ||
		secondAdministrator.Code != http.StatusServiceUnavailable ||
		secondUser.Body.String() != knowledgeManagementUnavailableBody ||
		secondAdministrator.Body.String() != knowledgeManagementUnavailableBody ||
		secondUserBody.reads() != 0 || administratorBody.reads() != 0 ||
		hookCalls.Load() != 1 || len(appender.snapshot()) != 0 {
		t.Fatalf(
			"user=%d/%q admin=%d/%q reads=%d/%d hook=%d calls=%+v",
			secondUser.Code,
			secondUser.Body.String(),
			secondAdministrator.Code,
			secondAdministrator.Body.String(),
			secondUserBody.reads(),
			administratorBody.reads(),
			hookCalls.Load(),
			appender.snapshot(),
		)
	}

	close(release)
	var first *httptest.ResponseRecorder
	select {
	case first = <-firstDone:
	case <-time.After(time.Second):
		t.Fatal("first rejected-attempt append did not complete")
	}
	if first.Code != http.StatusForbidden || len(appender.snapshot()) != 1 {
		t.Fatalf("first response=%d/%q calls=%+v", first.Code, first.Body.String(), appender.snapshot())
	}

	appender.setError(errors.New("journal unavailable"))
	afterSuccess := serveBounded(
		userBoundary,
		knowledgeObjectsGetPath,
		newKnowledgeBoundaryObservedBody("unread after success", nil),
	)
	if afterSuccess.Code != http.StatusServiceUnavailable ||
		hookCalls.Load() != 2 || len(appender.snapshot()) != 2 {
		t.Fatalf(
			"after-success response=%d/%q hook=%d calls=%+v",
			afterSuccess.Code,
			afterSuccess.Body.String(),
			hookCalls.Load(),
			appender.snapshot(),
		)
	}

	appender.setError(nil)
	afterError := serveBounded(
		userBoundary,
		knowledgeObjectsGetPath,
		newKnowledgeBoundaryObservedBody("unread after error", nil),
	)
	if afterError.Code != http.StatusForbidden ||
		hookCalls.Load() != 3 || len(appender.snapshot()) != 3 {
		t.Fatalf(
			"after-error response=%d/%q hook=%d calls=%+v",
			afterError.Code,
			afterError.Body.String(),
			hookCalls.Load(),
			appender.snapshot(),
		)
	}
}

func TestKnowledgeAttemptBoundaryUsesDetachedCancellationTailWithActor(t *testing.T) {
	t.Parallel()

	appender := &knowledgeBoundaryAppender{}
	handler := newKnowledgeBoundaryHandler(
		t,
		auth.BrowserRoleAdministrator,
		appender,
	)
	requestContext, cancel := context.WithCancel(context.Background())
	response := httptest.NewRecorder()
	handler.protectKnowledgeManagementRoutes(http.HandlerFunc(
		func(response http.ResponseWriter, request *http.Request) {
			cancel()
			err := rejectKnowledgeAttempt(
				request,
				knowledgeattemptaudit.ReasonInvalidDefinition,
				http.StatusBadRequest,
				"knowledge request is invalid",
			)
			writeKnowledgeBoundaryHTTPError(t, response, err)
		},
	)).ServeHTTP(response, knowledgeBoundaryRequest(
		requestContext,
		"/api/v1/knowledge/objects/update",
		nil,
	))

	calls := appender.snapshot()
	if len(calls) != 1 {
		t.Fatalf("append calls=%d, want 1", len(calls))
	}
	call := calls[0]
	if call.contextErr != nil || !call.actorOK ||
		call.actor.Kind != audit.ActorKindBrowser ||
		call.actor.ID != knowledgeBoundaryOwnerID ||
		call.actor.Role != audit.ActorRoleAdministrator ||
		!call.deadlineOK || time.Until(call.deadline) < 4*time.Second ||
		time.Until(call.deadline) > 6*time.Second ||
		response.Code != http.StatusBadRequest {
		t.Fatalf("call=%+v status=%d", call, response.Code)
	}
	wantTime, ok := audit.CanonicalOccurrenceTime(knowledgeBoundaryNow)
	if !ok || !call.definition.OccurredAt.Equal(wantTime) ||
		call.definition.OccurredAt.Location() != time.UTC {
		t.Fatalf(
			"OccurredAt=%v, want canonical %v",
			call.definition.OccurredAt,
			wantTime,
		)
	}
}

func TestKnowledgeAttemptBoundaryMapsUnhandledFailures(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		status int
		reason knowledgeattemptaudit.Reason
	}{
		{status: http.StatusBadRequest, reason: knowledgeattemptaudit.ReasonInvalidDefinition},
		{status: http.StatusUnsupportedMediaType, reason: knowledgeattemptaudit.ReasonInvalidDefinition},
		{status: http.StatusRequestEntityTooLarge, reason: knowledgeattemptaudit.ReasonResourceLimit},
		{status: http.StatusTooManyRequests, reason: knowledgeattemptaudit.ReasonResourceLimit},
		{status: http.StatusRequestTimeout, reason: knowledgeattemptaudit.ReasonServiceUnavailable},
		{status: http.StatusInternalServerError, reason: knowledgeattemptaudit.ReasonServiceUnavailable},
		{status: http.StatusServiceUnavailable, reason: knowledgeattemptaudit.ReasonServiceUnavailable},
	} {
		t.Run(http.StatusText(test.status), func(t *testing.T) {
			t.Parallel()
			appender := &knowledgeBoundaryAppender{}
			handler := newKnowledgeBoundaryHandler(
				t,
				auth.BrowserRoleAdministrator,
				appender,
			)
			response := httptest.NewRecorder()
			handler.protectKnowledgeManagementRoutes(http.HandlerFunc(
				func(response http.ResponseWriter, _ *http.Request) {
					response.Header().Add("X-Knowledge-Test", "first")
					response.Header().Add("X-Knowledge-Test", "second")
					response.WriteHeader(test.status)
					_, _ = response.Write([]byte("fixed original response"))
				},
			)).ServeHTTP(response, knowledgeBoundaryRequest(
				context.Background(),
				"/api/v1/knowledge/objects/set-state",
				nil,
			))

			calls := appender.snapshot()
			if len(calls) != 1 ||
				calls[0].definition.Action != knowledgeattemptaudit.ActionUpdate ||
				calls[0].definition.Reason != test.reason ||
				calls[0].definition.AuthorizedContext != nil ||
				response.Code != test.status ||
				response.Body.String() != "fixed original response" ||
				strings.Join(response.Header().Values("X-Knowledge-Test"), ",") != "first,second" {
				t.Fatalf(
					"calls=%+v status=%d headers=%v body=%q",
					calls,
					response.Code,
					response.Header(),
					response.Body.String(),
				)
			}
		})
	}
}

func TestKnowledgeAttemptBoundaryRefinesAndDetachesDefinitiveRejection(t *testing.T) {
	t.Parallel()

	var mu sync.Mutex
	var order []string
	appender := &knowledgeBoundaryAppender{
		hook: func(context.Context, string, knowledgeattemptaudit.Definition) {
			mu.Lock()
			order = append(order, "append")
			mu.Unlock()
		},
	}
	handler := newKnowledgeBoundaryHandler(
		t,
		auth.BrowserRoleAdministrator,
		appender,
	)
	response := httptest.NewRecorder()
	handler.protectKnowledgeManagementRoutes(http.HandlerFunc(
		func(response http.ResponseWriter, request *http.Request) {
			refineKnowledgeAttempt(
				request,
				knowledgeattemptaudit.ActionScopeChange,
			)
			authorized := &knowledgeattemptaudit.AuthorizedContext{
				AppID: "app-authorized",
				Object: &knowledgeattemptaudit.AuthorizedObject{
					KnowledgeObjectID: "ko-authorized",
					ObjectType:        knowledgeattemptaudit.ObjectTypeFieldAlias,
					Version:           11,
					SharingScope:      knowledgeattemptaudit.SharingScopeApp,
				},
			}
			setKnowledgeAttemptAuthorizedContext(request, authorized)
			authorized.AppID = "app-mutated"
			authorized.Object.KnowledgeObjectID = "ko-mutated"
			authorized.Object.Version = 99
			err := rejectKnowledgeAttempt(
				request,
				knowledgeattemptaudit.ReasonVersionConflict,
				http.StatusConflict,
				"knowledge object version conflict",
			)
			mu.Lock()
			order = append(order, "write")
			mu.Unlock()
			writeKnowledgeBoundaryHTTPError(t, response, err)
		},
	)).ServeHTTP(response, knowledgeBoundaryRequest(
		context.Background(),
		"/api/v1/knowledge/objects/update",
		nil,
	))

	calls := appender.snapshot()
	mu.Lock()
	observedOrder := append([]string(nil), order...)
	mu.Unlock()
	if len(calls) != 1 || strings.Join(observedOrder, ",") != "append,write" {
		t.Fatalf("calls=%+v order=%v", calls, observedOrder)
	}
	definition := calls[0].definition
	if definition.Action != knowledgeattemptaudit.ActionScopeChange ||
		definition.Reason != knowledgeattemptaudit.ReasonVersionConflict ||
		definition.AuthorizedContext == nil ||
		definition.AuthorizedContext.AppID != "app-authorized" ||
		definition.AuthorizedContext.Object == nil ||
		definition.AuthorizedContext.Object.KnowledgeObjectID != "ko-authorized" ||
		definition.AuthorizedContext.Object.Version != 11 ||
		response.Code != http.StatusConflict {
		t.Fatalf("definition=%+v status=%d", definition, response.Code)
	}
}

func TestKnowledgeAttemptBoundarySupportsMarkedHandlerRejection(t *testing.T) {
	t.Parallel()

	appender := &knowledgeBoundaryAppender{}
	handler := newKnowledgeBoundaryHandler(
		t,
		auth.BrowserRoleAdministrator,
		appender,
	)
	response := httptest.NewRecorder()
	handler.protectKnowledgeManagementRoutes(http.HandlerFunc(
		func(response http.ResponseWriter, request *http.Request) {
			markKnowledgeAttemptHandlerRejection(
				request,
				knowledgeattemptaudit.ReasonNotFoundOrForbidden,
			)
			response.WriteHeader(http.StatusNotFound)
		},
	)).ServeHTTP(response, knowledgeBoundaryRequest(
		context.Background(),
		"/api/v1/knowledge/objects/get",
		nil,
	))

	calls := appender.snapshot()
	if len(calls) != 1 ||
		calls[0].definition.Action != knowledgeattemptaudit.ActionGet ||
		calls[0].definition.Reason != knowledgeattemptaudit.ReasonNotFoundOrForbidden ||
		response.Code != http.StatusNotFound {
		t.Fatalf("calls=%+v status=%d", calls, response.Code)
	}
}

func TestKnowledgeAttemptBoundaryAppendFailureIsFixedAndNeverRetried(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name string
		role auth.BrowserRole
		next http.Handler
	}{
		{
			name: "ordinary user",
			role: auth.BrowserRoleUser,
			next: http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
				t.Fatal("ordinary user reached handler")
			}),
		},
		{
			name: "unhandled administrator error",
			role: auth.BrowserRoleAdministrator,
			next: http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
				response.Header().Set("X-Secret", "must disappear")
				response.Header().Set("Retry-After", "secret")
				response.WriteHeader(http.StatusBadRequest)
				_, _ = response.Write([]byte("secret original response"))
			}),
		},
		{
			name: "definitive administrator error",
			role: auth.BrowserRoleAdministrator,
			next: http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
				err := rejectKnowledgeAttempt(
					request,
					knowledgeattemptaudit.ReasonInvalidDefinition,
					http.StatusBadRequest,
					"secret requested response",
				)
				response.Header().Set("X-Secret", "must disappear")
				writeKnowledgeBoundaryHTTPError(t, response, err)
			}),
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			appender := &knowledgeBoundaryAppender{
				err: errors.New("journal contains secret backend detail"),
			}
			handler := newKnowledgeBoundaryHandler(t, test.role, appender)
			response := httptest.NewRecorder()
			handler.protectKnowledgeManagementRoutes(test.next).ServeHTTP(
				response,
				knowledgeBoundaryRequest(
					context.Background(),
					"/api/v1/knowledge/objects/create",
					nil,
				),
			)

			if len(appender.snapshot()) != 1 ||
				response.Code != http.StatusServiceUnavailable ||
				response.Body.String() != knowledgeManagementUnavailableBody ||
				response.Header().Get("Content-Type") != "application/json; charset=utf-8" ||
				response.Header().Get("Cache-Control") != "no-store" ||
				response.Header().Get("X-Secret") != "" ||
				response.Header().Get("Retry-After") != "" ||
				len(response.Header()) != 2 {
				t.Fatalf(
					"calls=%d status=%d headers=%v body=%q",
					len(appender.snapshot()),
					response.Code,
					response.Header(),
					response.Body.String(),
				)
			}
		})
	}
}

func TestKnowledgeAttemptBoundaryRejectsInvalidAuditProjectionBeforeAppender(t *testing.T) {
	t.Parallel()

	appender := &knowledgeBoundaryAppender{}
	handler := newKnowledgeBoundaryHandler(
		t,
		auth.BrowserRoleAdministrator,
		appender,
	)
	response := httptest.NewRecorder()
	handler.protectKnowledgeManagementRoutes(http.HandlerFunc(
		func(response http.ResponseWriter, request *http.Request) {
			// not_administrator is valid only for an authenticated ordinary user.
			// A programming error must fail closed before even a permissive test
			// appender can accept the malformed projection.
			err := rejectKnowledgeAttempt(
				request,
				knowledgeattemptaudit.ReasonNotAdministrator,
				http.StatusForbidden,
				"must not escape",
			)
			writeKnowledgeBoundaryHTTPError(t, response, err)
		},
	)).ServeHTTP(response, knowledgeBoundaryRequest(
		context.Background(),
		"/api/v1/knowledge/objects/update",
		nil,
	))

	if len(appender.snapshot()) != 0 ||
		response.Code != http.StatusServiceUnavailable ||
		response.Body.String() != knowledgeManagementUnavailableBody {
		t.Fatalf(
			"calls=%d status=%d body=%q",
			len(appender.snapshot()),
			response.Code,
			response.Body.String(),
		)
	}
}

func TestKnowledgeAttemptBoundaryStreamsSuccessfulResponses(t *testing.T) {
	t.Parallel()

	appender := &knowledgeBoundaryAppender{}
	handler := newKnowledgeBoundaryHandler(
		t,
		auth.BrowserRoleAdministrator,
		appender,
	)
	response := httptest.NewRecorder()
	visibleDuringHandler := false
	handler.protectKnowledgeManagementRoutes(http.HandlerFunc(
		func(writer http.ResponseWriter, _ *http.Request) {
			writer.Header().Set("X-Knowledge-Success", "yes")
			writer.WriteHeader(http.StatusOK)
			_, _ = writer.Write([]byte("success payload"))
			visibleDuringHandler = response.Code == http.StatusOK &&
				response.Body.String() == "success payload"
			flusher, ok := writer.(http.Flusher)
			if !ok {
				t.Fatal("knowledge response writer does not implement http.Flusher")
			}
			flusher.Flush()
		},
	)).ServeHTTP(response, knowledgeBoundaryRequest(
		context.Background(),
		"/api/v1/knowledge/objects/list",
		nil,
	))

	if !visibleDuringHandler || len(appender.snapshot()) != 0 ||
		response.Code != http.StatusOK ||
		response.Body.String() != "success payload" ||
		response.Header().Get("X-Knowledge-Success") != "yes" ||
		!response.Flushed {
		t.Fatalf(
			"visible=%v calls=%d status=%d headers=%v body=%q flushed=%v",
			visibleDuringHandler,
			len(appender.snapshot()),
			response.Code,
			response.Header(),
			response.Body.String(),
			response.Flushed,
		)
	}
}

func TestKnowledgeAttemptBoundarySuppressesPostMutationFailures(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name string
		mark func(*http.Request)
	}{
		{name: "committed", mark: markKnowledgeMutationCommitted},
		{name: "indeterminate", mark: markKnowledgeMutationIndeterminate},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			appender := &knowledgeBoundaryAppender{}
			handler := newKnowledgeBoundaryHandler(
				t,
				auth.BrowserRoleAdministrator,
				appender,
			)
			response := httptest.NewRecorder()
			handler.protectKnowledgeManagementRoutes(http.HandlerFunc(
				func(response http.ResponseWriter, request *http.Request) {
					test.mark(request)
					response.Header().Set("X-Mutation-Outcome", test.name)
					response.WriteHeader(http.StatusInternalServerError)
					_, _ = response.Write([]byte("serialization failed"))
				},
			)).ServeHTTP(response, knowledgeBoundaryRequest(
				context.Background(),
				"/api/v1/knowledge/objects/delete",
				nil,
			))

			if len(appender.snapshot()) != 0 ||
				response.Code != http.StatusInternalServerError ||
				response.Body.String() != "serialization failed" ||
				response.Header().Get("X-Mutation-Outcome") != test.name {
				t.Fatalf(
					"calls=%d status=%d headers=%v body=%q",
					len(appender.snapshot()),
					response.Code,
					response.Header(),
					response.Body.String(),
				)
			}
		})
	}
}

func TestKnowledgeAttemptBoundaryBoundsStagedErrorsAndFailsGeneric(t *testing.T) {
	t.Parallel()

	appender := &knowledgeBoundaryAppender{}
	handler := newKnowledgeBoundaryHandler(
		t,
		auth.BrowserRoleAdministrator,
		appender,
	)
	response := httptest.NewRecorder()
	handler.protectKnowledgeManagementRoutes(http.HandlerFunc(
		func(response http.ResponseWriter, _ *http.Request) {
			response.Header().Set("X-Secret", "must disappear")
			response.WriteHeader(http.StatusBadRequest)
			payload := strings.Repeat(
				"s",
				maximumKnowledgeStagedErrorBytes+1,
			)
			written, err := response.Write([]byte(payload))
			if err != nil || written != len(payload) {
				t.Fatalf("staged overflow write=(%d,%v), want (%d,nil)", written, err, len(payload))
			}
		},
	)).ServeHTTP(response, knowledgeBoundaryRequest(
		context.Background(),
		"/api/v1/knowledge/objects/create",
		nil,
	))

	calls := appender.snapshot()
	if len(calls) != 1 ||
		calls[0].definition.Reason != knowledgeattemptaudit.ReasonInvalidDefinition ||
		response.Code != http.StatusServiceUnavailable ||
		response.Body.String() != knowledgeManagementUnavailableBody ||
		response.Header().Get("X-Secret") != "" {
		t.Fatalf(
			"calls=%+v status=%d headers=%v body=%q",
			calls,
			response.Code,
			response.Header(),
			response.Body.String(),
		)
	}
}

func TestKnowledgeAttemptBoundaryUnauthenticatedFailureIsNotJournaled(t *testing.T) {
	t.Parallel()

	appender := &knowledgeBoundaryAppender{}
	handler := newKnowledgeBoundaryHandler(
		t,
		auth.BrowserRoleAdministrator,
		appender,
	)
	body := newKnowledgeBoundaryObservedBody("must remain unread", nil)
	request := httptest.NewRequestWithContext(
		t.Context(),
		http.MethodPost,
		"/api/v1/knowledge/objects/create",
		body,
	)
	response := httptest.NewRecorder()
	handler.protectKnowledgeManagementRoutes(http.HandlerFunc(
		func(http.ResponseWriter, *http.Request) {
			t.Fatal("unauthenticated request reached handler")
		},
	)).ServeHTTP(response, request)

	if response.Code != http.StatusUnauthorized || body.reads() != 0 ||
		len(appender.snapshot()) != 0 {
		t.Fatalf(
			"status=%d reads=%d calls=%d body=%q",
			response.Code,
			body.reads(),
			len(appender.snapshot()),
			response.Body.String(),
		)
	}
}
