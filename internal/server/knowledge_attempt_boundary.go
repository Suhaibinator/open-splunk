package server

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/Suhaibinator/SRouter/pkg/router"
	"github.com/Suhaibinator/open-splunk/internal/audit"
	"github.com/Suhaibinator/open-splunk/internal/knowledgeattemptaudit"
)

const (
	knowledgeAttemptAppendTimeout      = 5 * time.Second
	maximumKnowledgeStagedErrorBytes   = 16 << 10
	knowledgeManagementUnavailableText = "knowledge management is unavailable"
)

const (
	knowledgeManagementUnavailableBody = "{\"error\":{\"message\":\"knowledge management is unavailable\"}}\n"
	knowledgeAdministratorRequiredBody = "{\"error\":{\"message\":\"administrator access is required\"}}\n"
)

var errKnowledgeAttemptBoundaryUnavailable = errors.New(
	"knowledge attempt boundary is unavailable",
)

type knowledgeAttemptStateContextKey struct{}

type knowledgeAttemptState struct {
	mu sync.Mutex

	action            knowledgeattemptaudit.Action
	authorizedContext *knowledgeattemptaudit.AuthorizedContext
	reason            knowledgeattemptaudit.Reason
	definitive        bool
	invalid           bool
	suppressed        bool
	appendStarted     bool
	appendErr         error
	append            func(knowledgeattemptaudit.Definition) error
}

// protectKnowledgeManagementRoutes is the authenticated, fail-closed attempt
// boundary for the exact Tier-1 knowledge-management surface. Host and Origin
// validation belongs immediately outside this middleware. Keeping route
// recognition here exact prevents a misspelled path or method from becoming an
// authenticated (and therefore journaled) privileged attempt.
func (handler *apiHandler) protectKnowledgeManagementRoutes(
	next http.Handler,
) http.Handler {
	return http.HandlerFunc(func(
		response http.ResponseWriter,
		request *http.Request,
	) {
		action, protected := knowledgeAttemptFallbackAction(request)
		if !protected && handler.knowledgePreviewConfigured() &&
			request != nil && request.Method == http.MethodPost &&
			request.URL != nil && request.URL.Path == knowledgeObjectsPreviewPath {
			action, protected = knowledgeattemptaudit.ActionPreview, true
		}
		if !protected {
			next.ServeHTTP(response, request)
			return
		}
		if handler == nil || handler.routeTimeout <= 0 {
			writeFixedKnowledgeUnavailable(response)
			return
		}
		routeContext, cancel := context.WithTimeout(
			request.Context(),
			handler.routeTimeout,
		)
		defer cancel()
		request = request.WithContext(routeContext)

		authenticatedRequest, principal, authenticated := handler.authenticateBrowser(
			response,
			request,
		)
		if !authenticated {
			return
		}

		authenticatedContext := authenticatedRequest.Context()
		authenticatedTenantID := principal.TenantID()
		appendAttempt := func(
			definition knowledgeattemptaudit.Definition,
		) error {
			return handler.appendKnowledgeRejectedAttempt(
				authenticatedContext,
				authenticatedTenantID,
				definition,
			)
		}
		if !principal.IsAdministrator() {
			if err := appendAttempt(knowledgeattemptaudit.Definition{
				Action: action,
				Reason: knowledgeattemptaudit.ReasonNotAdministrator,
			}); err != nil {
				writeFixedKnowledgeUnavailable(response)
				return
			}
			writeFixedKnowledgeAdministratorRequired(response)
			return
		}
		if handler == nil || principal.TenantID() != handler.tenantID ||
			principal.OwnerID() != handler.ownerID {
			if err := appendAttempt(knowledgeattemptaudit.Definition{
				Action: action,
				Reason: knowledgeattemptaudit.ReasonNotFoundOrForbidden,
			}); err != nil {
				writeFixedKnowledgeUnavailable(response)
				return
			}
			writeFixedKnowledgeAdministratorRequired(response)
			return
		}

		attempt := &knowledgeAttemptState{
			action: action,
			append: appendAttempt,
		}
		authenticatedRequest = authenticatedRequest.WithContext(
			context.WithValue(
				authenticatedRequest.Context(),
				knowledgeAttemptStateContextKey{},
				attempt,
			),
		)
		staged := newKnowledgeAttemptResponseWriter(response)
		next.ServeHTTP(staged, authenticatedRequest)
		staged.finish(attempt)
	})
}

func knowledgeAttemptFallbackAction(
	request *http.Request,
) (knowledgeattemptaudit.Action, bool) {
	if request == nil || request.Method != http.MethodPost || request.URL == nil {
		return "", false
	}
	switch request.URL.Path {
	case knowledgeObjectsCreatePath:
		return knowledgeattemptaudit.ActionCreate, true
	case knowledgeObjectsGetPath:
		return knowledgeattemptaudit.ActionGet, true
	case knowledgeObjectsListPath:
		return knowledgeattemptaudit.ActionList, true
	case knowledgeObjectsDependenciesPath:
		return knowledgeattemptaudit.ActionDependencies, true
	case knowledgeObjectsDependentsPath:
		return knowledgeattemptaudit.ActionDependents, true
	case knowledgeObjectsValidatePath:
		return knowledgeattemptaudit.ActionValidate, true
	case knowledgeObjectsUpdatePath,
		knowledgeObjectsSetStatePath:
		return knowledgeattemptaudit.ActionUpdate, true
	case knowledgeObjectsDeletePath:
		return knowledgeattemptaudit.ActionDelete, true
	default:
		return "", false
	}
}

func (handler *apiHandler) appendKnowledgeRejectedAttempt(
	authenticatedContext context.Context,
	tenantID string,
	definition knowledgeattemptaudit.Definition,
) error {
	if authenticatedContext == nil || handler == nil || handler.now == nil ||
		isNilDependency(handler.knowledgeAttempts) {
		return errKnowledgeAttemptBoundaryUnavailable
	}
	select {
	case handler.knowledgeAttemptGate <- struct{}{}:
		defer func() { <-handler.knowledgeAttemptGate }()
	default:
		return errKnowledgeAttemptBoundaryUnavailable
	}
	if !knowledgeAttemptActionValid(definition.Action) ||
		!knowledgeAttemptReasonValid(definition.Reason) {
		return errKnowledgeAttemptBoundaryUnavailable
	}
	occurredAt, ok := audit.CanonicalOccurrenceTime(handler.now())
	if !ok {
		return errKnowledgeAttemptBoundaryUnavailable
	}
	definition.OccurredAt = occurredAt
	definition.AuthorizedContext = cloneKnowledgeAttemptAuthorizedContext(
		definition.AuthorizedContext,
	)
	actor, ok := audit.ActorFromContext(authenticatedContext)
	if !ok {
		return errKnowledgeAttemptBoundaryUnavailable
	}
	if err := (knowledgeattemptaudit.Event{
		Sequence:          1,
		TenantID:          tenantID,
		OccurredAt:        occurredAt,
		Actor:             actor,
		Action:            definition.Action,
		Result:            knowledgeattemptaudit.ResultRejected,
		Reason:            definition.Reason,
		AuthorizedContext: definition.AuthorizedContext,
	}).ValidateForTenant(tenantID); err != nil {
		return errKnowledgeAttemptBoundaryUnavailable
	}

	// Rejection journaling is a durable tail action. It retains the authenticated
	// actor value but deliberately ignores client cancellation and the ordinary
	// route deadline. The independent bound prevents a failed journal from
	// holding the response indefinitely.
	tailContext, cancel := context.WithTimeout(
		context.WithoutCancel(authenticatedContext),
		knowledgeAttemptAppendTimeout,
	)
	defer cancel()
	return handler.knowledgeAttempts.AppendRejected(
		tailContext,
		strings.Clone(tenantID),
		definition,
	)
}

func knowledgeAttemptStateFromRequest(
	request *http.Request,
) (*knowledgeAttemptState, bool) {
	if request == nil {
		return nil, false
	}
	state, ok := request.Context().Value(
		knowledgeAttemptStateContextKey{},
	).(*knowledgeAttemptState)
	return state, ok && state != nil
}

func knowledgeAttemptActionFromRequest(
	request *http.Request,
) (knowledgeattemptaudit.Action, bool) {
	state, ok := knowledgeAttemptStateFromRequest(request)
	if !ok {
		return "", false
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if !knowledgeAttemptActionValid(state.action) {
		return "", false
	}
	return state.action, true
}

// refineKnowledgeAttempt replaces the pre-decode route action with the exact
// semantic action derived from a validated request (for example enable,
// disable, or scope_change).
func refineKnowledgeAttempt(
	request *http.Request,
	action knowledgeattemptaudit.Action,
) {
	state, ok := knowledgeAttemptStateFromRequest(request)
	if !ok {
		return
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.appendStarted || state.suppressed {
		return
	}
	if !knowledgeAttemptActionValid(action) {
		state.invalid = true
		return
	}
	state.action = action
}

// setKnowledgeAttemptAuthorizedContext records only already-authorized,
// scalar publication metadata. The detached clone prevents later request or
// service mutations from changing what is journaled.
func setKnowledgeAttemptAuthorizedContext(
	request *http.Request,
	authorizedContext *knowledgeattemptaudit.AuthorizedContext,
) {
	state, ok := knowledgeAttemptStateFromRequest(request)
	if !ok {
		return
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.appendStarted || state.suppressed {
		return
	}
	state.authorizedContext = cloneKnowledgeAttemptAuthorizedContext(
		authorizedContext,
	)
}

// markKnowledgeAttemptHandlerRejection supplies the exact reason for a
// definitive handler rejection. rejectKnowledgeAttempt is preferred because
// it also performs the synchronous append before returning an HTTP error.
func markKnowledgeAttemptHandlerRejection(
	request *http.Request,
	reason knowledgeattemptaudit.Reason,
) {
	state, ok := knowledgeAttemptStateFromRequest(request)
	if !ok {
		return
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.appendStarted || state.suppressed {
		return
	}
	if !knowledgeAttemptReasonValid(reason) {
		state.invalid = true
		return
	}
	state.reason = reason
	state.definitive = true
}

// rejectKnowledgeAttempt durably appends one exact handler rejection before
// SRouter receives the HTTP error. The outer response boundary observes the
// append result and therefore neither duplicates the event nor exposes the
// requested error if the append failed.
func rejectKnowledgeAttempt(
	request *http.Request,
	reason knowledgeattemptaudit.Reason,
	status int,
	message string,
) error {
	state, ok := knowledgeAttemptStateFromRequest(request)
	if !ok || !knowledgeAttemptReasonValid(reason) {
		return router.NewHTTPError(
			http.StatusServiceUnavailable,
			knowledgeManagementUnavailableText,
		)
	}
	if _, err := state.appendRejected(reason, true); err != nil {
		return router.NewHTTPError(
			http.StatusServiceUnavailable,
			knowledgeManagementUnavailableText,
		)
	}
	return router.NewHTTPError(status, message)
}

// markKnowledgeMutationCommitted suppresses a second, non-atomic rejection
// event when a mutation has already committed its successful mutation audit
// but response serialization subsequently fails.
func markKnowledgeMutationCommitted(request *http.Request) {
	markKnowledgeMutationOutcome(request)
}

// markKnowledgeMutationIndeterminate suppresses a rejected-attempt event when
// the mutation boundary cannot prove that no commit occurred. Recovery owns
// resolution of that outcome; retrying the attempt journal could misclassify a
// committed mutation as rejected.
func markKnowledgeMutationIndeterminate(request *http.Request) {
	markKnowledgeMutationOutcome(request)
}

func markKnowledgeMutationOutcome(request *http.Request) {
	state, ok := knowledgeAttemptStateFromRequest(request)
	if !ok {
		return
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.appendStarted {
		return
	}
	state.suppressed = true
	state.authorizedContext = nil
}

func (state *knowledgeAttemptState) appendRejected(
	reason knowledgeattemptaudit.Reason,
	definitive bool,
) (bool, error) {
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.suppressed {
		return true, nil
	}
	if state.appendStarted {
		return false, state.appendErr
	}
	state.appendStarted = true
	state.reason = reason
	if definitive {
		state.definitive = true
	}
	if state.invalid || !knowledgeAttemptActionValid(state.action) ||
		!knowledgeAttemptReasonValid(state.reason) || state.append == nil {
		state.appendErr = errKnowledgeAttemptBoundaryUnavailable
		return false, state.appendErr
	}
	state.appendErr = state.append(knowledgeattemptaudit.Definition{
		Action: state.action,
		Reason: state.reason,
		AuthorizedContext: cloneKnowledgeAttemptAuthorizedContext(
			state.authorizedContext,
		),
	})
	return false, state.appendErr
}

func (state *knowledgeAttemptState) finishRejectedResponse(
	status int,
) (suppressed bool, appendFailed bool) {
	state.mu.Lock()
	definitive := state.definitive
	reason := state.reason
	state.mu.Unlock()
	if !definitive {
		reason = fallbackKnowledgeAttemptReason(status)
	}
	suppressed, err := state.appendRejected(reason, definitive)
	return suppressed, err != nil
}

func fallbackKnowledgeAttemptReason(
	status int,
) knowledgeattemptaudit.Reason {
	switch status {
	case http.StatusBadRequest, http.StatusUnsupportedMediaType:
		return knowledgeattemptaudit.ReasonInvalidDefinition
	case http.StatusRequestEntityTooLarge, http.StatusTooManyRequests:
		return knowledgeattemptaudit.ReasonResourceLimit
	case http.StatusRequestTimeout:
		return knowledgeattemptaudit.ReasonServiceUnavailable
	default:
		// Unexpected redirects, authorization statuses, and handler errors all
		// collapse to the same closed infrastructure reason. Exact application
		// rejections must use rejectKnowledgeAttempt instead.
		return knowledgeattemptaudit.ReasonServiceUnavailable
	}
}

func knowledgeAttemptActionValid(action knowledgeattemptaudit.Action) bool {
	switch action {
	case knowledgeattemptaudit.ActionCreate,
		knowledgeattemptaudit.ActionGet,
		knowledgeattemptaudit.ActionList,
		knowledgeattemptaudit.ActionUpdate,
		knowledgeattemptaudit.ActionScopeChange,
		knowledgeattemptaudit.ActionEnable,
		knowledgeattemptaudit.ActionDisable,
		knowledgeattemptaudit.ActionQuarantine,
		knowledgeattemptaudit.ActionDelete,
		knowledgeattemptaudit.ActionValidate,
		knowledgeattemptaudit.ActionDependencies,
		knowledgeattemptaudit.ActionDependents,
		knowledgeattemptaudit.ActionPreview:
		return true
	default:
		return false
	}
}

func knowledgeAttemptReasonValid(reason knowledgeattemptaudit.Reason) bool {
	switch reason {
	case knowledgeattemptaudit.ReasonNotAdministrator,
		knowledgeattemptaudit.ReasonNotFoundOrForbidden,
		knowledgeattemptaudit.ReasonVersionConflict,
		knowledgeattemptaudit.ReasonIdempotencyConflict,
		knowledgeattemptaudit.ReasonInvalidDefinition,
		knowledgeattemptaudit.ReasonForbiddenDependency,
		knowledgeattemptaudit.ReasonResourceLimit,
		knowledgeattemptaudit.ReasonServiceUnavailable:
		return true
	default:
		return false
	}
}

func cloneKnowledgeAttemptAuthorizedContext(
	value *knowledgeattemptaudit.AuthorizedContext,
) *knowledgeattemptaudit.AuthorizedContext {
	if value == nil {
		return nil
	}
	cloned := &knowledgeattemptaudit.AuthorizedContext{
		AppID: strings.Clone(value.AppID),
	}
	if value.Object != nil {
		cloned.Object = &knowledgeattemptaudit.AuthorizedObject{
			KnowledgeObjectID: strings.Clone(
				value.Object.KnowledgeObjectID,
			),
			ObjectType:   value.Object.ObjectType,
			Version:      value.Object.Version,
			SharingScope: value.Object.SharingScope,
		}
	}
	return cloned
}

type knowledgeAttemptResponseWriter struct {
	destination     http.ResponseWriter
	header          http.Header
	committedHeader http.Header
	status          int
	decided         bool
	streaming       bool
	overflowed      bool
	body            bytes.Buffer
}

func newKnowledgeAttemptResponseWriter(
	destination http.ResponseWriter,
) *knowledgeAttemptResponseWriter {
	return &knowledgeAttemptResponseWriter{
		destination: destination,
		header:      destination.Header().Clone(),
	}
}

func (writer *knowledgeAttemptResponseWriter) Header() http.Header {
	return writer.header
}

func (writer *knowledgeAttemptResponseWriter) WriteHeader(status int) {
	if writer.decided {
		return
	}
	writer.decided = true
	writer.status = status
	writer.committedHeader = writer.header.Clone()
	if status >= http.StatusOK && status < http.StatusMultipleChoices {
		writer.streaming = true
		replaceKnowledgeResponseHeaders(
			writer.destination.Header(),
			writer.committedHeader,
		)
		writer.destination.WriteHeader(status)
	}
}

func (writer *knowledgeAttemptResponseWriter) Write(payload []byte) (int, error) {
	if !writer.decided {
		writer.WriteHeader(http.StatusOK)
	}
	if writer.streaming {
		return writer.destination.Write(payload)
	}
	if writer.overflowed || len(payload) >
		maximumKnowledgeStagedErrorBytes-writer.body.Len() {
		writer.overflowed = true
		writer.body.Reset()
		return len(payload), nil
	}
	return writer.body.Write(payload)
}

// Flush preserves streaming semantics for successful responses. A staged
// rejection is intentionally not observable until its journal append commits.
func (writer *knowledgeAttemptResponseWriter) Flush() {
	if !writer.decided {
		writer.WriteHeader(http.StatusOK)
	}
	if writer.streaming {
		if flusher, ok := writer.destination.(http.Flusher); ok {
			flusher.Flush()
		}
	}
}

func (writer *knowledgeAttemptResponseWriter) finish(
	state *knowledgeAttemptState,
) {
	if !writer.decided {
		writer.WriteHeader(http.StatusOK)
	}
	if writer.streaming {
		return
	}
	_, appendFailed := state.finishRejectedResponse(writer.status)
	if appendFailed || writer.overflowed {
		writeFixedKnowledgeUnavailable(writer.destination)
		return
	}
	writer.commitStagedError()
}

func (writer *knowledgeAttemptResponseWriter) commitStagedError() {
	replaceKnowledgeResponseHeaders(
		writer.destination.Header(),
		writer.committedHeader,
	)
	writer.destination.WriteHeader(writer.status)
	if writer.body.Len() != 0 {
		_, _ = writer.destination.Write(writer.body.Bytes())
	}
}

func replaceKnowledgeResponseHeaders(destination, source http.Header) {
	for name := range destination {
		delete(destination, name)
	}
	for name, values := range source {
		destination[name] = append([]string(nil), values...)
	}
}

func writeFixedKnowledgeAdministratorRequired(response http.ResponseWriter) {
	replaceKnowledgeResponseHeaders(response.Header(), http.Header{
		"Cache-Control": []string{"no-store"},
		"Content-Type":  []string{"application/json; charset=utf-8"},
	})
	response.WriteHeader(http.StatusForbidden)
	_, _ = response.Write([]byte(knowledgeAdministratorRequiredBody))
}

func writeFixedKnowledgeUnavailable(response http.ResponseWriter) {
	replaceKnowledgeResponseHeaders(response.Header(), http.Header{
		"Cache-Control": []string{"no-store"},
		"Content-Type":  []string{"application/json; charset=utf-8"},
	})
	response.WriteHeader(http.StatusServiceUnavailable)
	_, _ = response.Write([]byte(knowledgeManagementUnavailableBody))
}
