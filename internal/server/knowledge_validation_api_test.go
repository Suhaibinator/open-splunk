package server

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/Suhaibinator/SRouter/pkg/router"
	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/audit"
	"github.com/Suhaibinator/open-splunk/internal/auth"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/knowledgeattemptaudit"
	"github.com/Suhaibinator/open-splunk/internal/knowledgecatalog"
	"github.com/Suhaibinator/open-splunk/internal/knowledgevalidation"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

type knowledgeValidationAttemptLog struct {
	mu          sync.Mutex
	definitions []knowledgeattemptaudit.Definition
}

func (log *knowledgeValidationAttemptLog) append(
	definition knowledgeattemptaudit.Definition,
) error {
	log.mu.Lock()
	defer log.mu.Unlock()
	definition.AuthorizedContext = cloneKnowledgeAttemptAuthorizedContext(
		definition.AuthorizedContext,
	)
	log.definitions = append(log.definitions, definition)
	return nil
}

func (log *knowledgeValidationAttemptLog) snapshot() []knowledgeattemptaudit.Definition {
	log.mu.Lock()
	defer log.mu.Unlock()
	result := make([]knowledgeattemptaudit.Definition, len(log.definitions))
	for index, definition := range log.definitions {
		result[index] = definition
		result[index].AuthorizedContext = cloneKnowledgeAttemptAuthorizedContext(
			definition.AuthorizedContext,
		)
	}
	return result
}

func knowledgeValidationDirectRequest(
	t *testing.T,
	appendAttempt func(context.Context, knowledgeattemptaudit.Definition) error,
) *http.Request {
	t.Helper()
	request := knowledgeHTTPDirectAdministratorRequest(
		t,
		knowledgeObjectsValidatePath,
	)
	ctx, err := audit.WithActor(request.Context(), audit.Actor{
		Kind: audit.ActorKindBrowser,
		ID:   knowledgeBoundaryOwnerID,
		Role: audit.ActorRoleAdministrator,
	})
	if err != nil {
		t.Fatalf("audit.WithActor: %v", err)
	}
	state := &knowledgeAttemptState{action: knowledgeattemptaudit.ActionValidate}
	state.append = func(definition knowledgeattemptaudit.Definition) error {
		if appendAttempt == nil {
			return nil
		}
		return appendAttempt(ctx, definition)
	}
	return request.WithContext(context.WithValue(
		ctx,
		knowledgeAttemptStateContextKey{},
		state,
	))
}

func knowledgeValidationCreateRequest(
	definition *opensplunk.KnowledgeObjectDefinition,
) *opensplunk.ValidateKnowledgeObjectRequest {
	return &opensplunk.ValidateKnowledgeObjectRequest{
		Definition: definition,
		Intent:     opensplunk.KnowledgeValidationIntent_KNOWLEDGE_VALIDATION_INTENT_INACTIVE_STORAGE,
	}
}

func knowledgeValidationHTTPStatus(t *testing.T, err error) int {
	t.Helper()
	var httpErr *router.HTTPError
	if !errors.As(err, &httpErr) || httpErr == nil {
		t.Fatalf("error = %T %v, want router.HTTPError", err, err)
	}
	return httpErr.StatusCode
}

type knowledgeValidationPersistenceCounts struct {
	objects       int64
	versions      int64
	definitions   int64
	dependencies  int64
	projections   int64
	selectors     int64
	tenants       int64
	receipts      int64
	commitRecords int64
	auditEvents   int64
}

func readKnowledgeValidationPersistenceCounts(
	t *testing.T,
	harness *knowledgePersistenceHarness,
) knowledgeValidationPersistenceCounts {
	t.Helper()
	var result knowledgeValidationPersistenceCounts
	err := harness.database.SQLDB().QueryRowContext(t.Context(), `
		SELECT
			(SELECT count(*) FROM knowledge_objects),
			(SELECT count(*) FROM knowledge_object_versions),
			(SELECT count(*) FROM knowledge_definition_blobs),
			(SELECT count(*) FROM knowledge_object_dependencies),
			(SELECT count(*) FROM knowledge_object_list_projections),
			(SELECT count(*) FROM knowledge_object_list_selector_patterns),
			(SELECT count(*) FROM knowledge_catalog_tenants),
			(SELECT count(*) FROM knowledge_mutation_idempotency),
			(SELECT count(*) FROM knowledge_mutation_commit_authorities),
			(SELECT count(*) FROM audit_events)`).Scan(
		&result.objects,
		&result.versions,
		&result.definitions,
		&result.dependencies,
		&result.projections,
		&result.selectors,
		&result.tenants,
		&result.receipts,
		&result.commitRecords,
		&result.auditEvents,
	)
	if err != nil {
		t.Fatalf("read validation persistence counts: %v", err)
	}
	return result
}

func TestKnowledgeValidateHandlerReturnsSealedResultsWithoutSideEffects(
	t *testing.T,
) {
	harness := newKnowledgePersistenceHarness(t, nil)
	before := readKnowledgeValidationPersistenceCounts(t, harness)

	tests := []struct {
		name       string
		definition *opensplunk.KnowledgeObjectDefinition
		wantValid  bool
	}{
		{
			name: "valid inactive candidate",
			definition: knowledgeHTTPDefinition(
				opensplunk.SharingScope_SHARING_SCOPE_PRIVATE,
			),
			wantValid: true,
		},
		{
			name: "invalid candidate remains in band",
			definition: &opensplunk.KnowledgeObjectDefinition{
				AppId:        knowledgeHTTPAppID,
				Name:         "invalid-validation-candidate",
				SharingScope: opensplunk.SharingScope_SHARING_SCOPE_PRIVATE,
				Body: &opensplunk.KnowledgeObjectDefinition_FieldAlias{
					FieldAlias: &opensplunk.FieldAliasDefinition{},
				},
			},
			wantValid: false,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			attempts := &knowledgeValidationAttemptLog{}
			request := knowledgeValidationDirectRequest(
				t,
				func(_ context.Context, definition knowledgeattemptaudit.Definition) error {
					return attempts.append(definition)
				},
			)
			serialized, err := harness.handler.validateKnowledgeObject(
				request,
				knowledgeValidationCreateRequest(test.definition),
			)
			if err != nil {
				t.Fatalf("validateKnowledgeObject: %v", err)
			}
			if serialized == nil || len(harness.handler.serializationGate) != 1 {
				t.Fatalf(
					"serialized=%v gate=%d, want response holding one permit",
					serialized,
					len(harness.handler.serializationGate),
				)
			}
			projection, err := serialized.sealed.Proto(request.Context())
			if err != nil || projection.GetResult().GetValid() != test.wantValid {
				t.Fatalf("sealed projection=%v error=%v", projection, err)
			}
			wire := serialized.sealed.DeterministicBytes()
			response := httptest.NewRecorder()
			if err := newValidateKnowledgeObjectCodec().Encode(response, serialized); err != nil {
				t.Fatalf("Encode: %v", err)
			}
			if !bytes.Equal(response.Body.Bytes(), wire) ||
				len(harness.handler.serializationGate) != 0 {
				t.Fatalf(
					"encoded=%x want=%x gate=%d",
					response.Body.Bytes(),
					wire,
					len(harness.handler.serializationGate),
				)
			}
			if got := attempts.snapshot(); len(got) != 0 {
				t.Fatalf("successful validation attempts=%+v, want none", got)
			}
		})
	}

	after := readKnowledgeValidationPersistenceCounts(t, harness)
	if !reflect.DeepEqual(after, before) {
		t.Fatalf("validation changed persistence:\nbefore=%#v\nafter=%#v", before, after)
	}
	if count := knowledgePersistenceCount(t, harness.database, `
		SELECT count(*) FROM knowledge_attempt_audit_events WHERE tenant_id = ?`,
		knowledgeBoundaryTenantID,
	); count != 0 {
		t.Fatalf("successful/in-band validation attempt rows=%d, want 0", count)
	}
}

func TestKnowledgeValidateHandlerTransfersLiveContextWithPermit(t *testing.T) {
	harness := newKnowledgePersistenceHarness(t, nil)
	request := knowledgeValidationDirectRequest(t, nil)
	ctx, cancel := context.WithCancel(request.Context())
	request = request.WithContext(ctx)
	serialized, err := harness.handler.validateKnowledgeObject(
		request,
		knowledgeValidationCreateRequest(knowledgeHTTPDefinition(
			opensplunk.SharingScope_SHARING_SCOPE_PRIVATE,
		)),
	)
	if err != nil || serialized == nil || len(harness.handler.serializationGate) != 1 {
		t.Fatalf(
			"response=%v error=%v gate=%d",
			serialized,
			err,
			len(harness.handler.serializationGate),
		)
	}
	cancel()
	if serialized.ctx != request.Context() || !errors.Is(serialized.ctx.Err(), context.Canceled) {
		t.Fatalf("serialized context=%v, want exact live canceled request context", serialized.ctx)
	}
	if err := newValidateKnowledgeObjectCodec().Encode(
		httptest.NewRecorder(),
		serialized,
	); !errors.Is(err, context.Canceled) {
		t.Fatalf("Encode error=%v, want context.Canceled", err)
	}
	if len(harness.handler.serializationGate) != 0 {
		t.Fatalf("serialization gate=%d, want released", len(harness.handler.serializationGate))
	}
}

func TestKnowledgeValidateHandlerRejectsEnvelopeBeforeAdmissionAndApps(
	t *testing.T,
) {
	harness := newKnowledgePersistenceHarness(t, nil)
	attempts := &knowledgeValidationAttemptLog{}
	request := knowledgeValidationDirectRequest(
		t,
		func(_ context.Context, definition knowledgeattemptaudit.Definition) error {
			return attempts.append(definition)
		},
	)
	_, err := harness.handler.validateKnowledgeObject(
		request,
		&opensplunk.ValidateKnowledgeObjectRequest{},
	)
	if status := knowledgeValidationHTTPStatus(t, err); status != http.StatusBadRequest {
		t.Fatalf("status=%d, want %d", status, http.StatusBadRequest)
	}
	if len(harness.handler.serializationGate) != 0 || harness.apps.callCount() != 0 {
		t.Fatalf(
			"gate=%d app calls=%d, want envelope rejection before both",
			len(harness.handler.serializationGate),
			harness.apps.callCount(),
		)
	}
	got := attempts.snapshot()
	if len(got) != 1 || got[0].Action != knowledgeattemptaudit.ActionValidate ||
		got[0].Reason != knowledgeattemptaudit.ReasonInvalidDefinition {
		t.Fatalf("attempts=%+v", got)
	}
}

func TestKnowledgeValidateHandlerAdmissionPrecedesCandidateAuthority(
	t *testing.T,
) {
	harness := newKnowledgePersistenceHarness(t, nil)
	for range cap(harness.handler.serializationGate) {
		harness.handler.serializationGate <- struct{}{}
	}
	t.Cleanup(func() {
		for range cap(harness.handler.serializationGate) {
			<-harness.handler.serializationGate
		}
	})
	attempts := &knowledgeValidationAttemptLog{}
	definition := knowledgeHTTPDefinition(
		opensplunk.SharingScope_SHARING_SCOPE_PRIVATE,
	)
	definition.AppId = strings.Repeat(" ", 4<<20)
	_, err := harness.handler.validateKnowledgeObject(
		knowledgeValidationDirectRequest(
			t,
			func(_ context.Context, definition knowledgeattemptaudit.Definition) error {
				return attempts.append(definition)
			},
		),
		knowledgeValidationCreateRequest(definition),
	)
	if status := knowledgeValidationHTTPStatus(t, err); status != http.StatusTooManyRequests {
		t.Fatalf("status=%d, want %d", status, http.StatusTooManyRequests)
	}
	if harness.apps.callCount() != 0 ||
		len(harness.handler.serializationGate) != cap(harness.handler.serializationGate) {
		t.Fatalf(
			"app calls=%d gate=%d/%d",
			harness.apps.callCount(),
			len(harness.handler.serializationGate),
			cap(harness.handler.serializationGate),
		)
	}
	got := attempts.snapshot()
	if len(got) != 1 || got[0].Reason != knowledgeattemptaudit.ReasonResourceLimit {
		t.Fatalf("attempts=%+v", got)
	}
}

func TestKnowledgeValidateHandlerRequiresExactWriterBeforeAppCatalog(
	t *testing.T,
) {
	harness := newKnowledgePersistenceHarness(t, nil)
	harness.handler.knowledgeWriter = struct{ KnowledgeWriter }{
		KnowledgeWriter: harness.writer,
	}
	attempts := &knowledgeValidationAttemptLog{}
	_, err := harness.handler.validateKnowledgeObject(
		knowledgeValidationDirectRequest(
			t,
			func(_ context.Context, definition knowledgeattemptaudit.Definition) error {
				return attempts.append(definition)
			},
		),
		knowledgeValidationCreateRequest(knowledgeHTTPDefinition(
			opensplunk.SharingScope_SHARING_SCOPE_PRIVATE,
		)),
	)
	if status := knowledgeValidationHTTPStatus(t, err); status != http.StatusServiceUnavailable {
		t.Fatalf("status=%d, want %d", status, http.StatusServiceUnavailable)
	}
	if harness.apps.callCount() != 0 || len(harness.handler.serializationGate) != 0 {
		t.Fatalf(
			"app calls=%d gate=%d, want no callback and released permit",
			harness.apps.callCount(),
			len(harness.handler.serializationGate),
		)
	}
	got := attempts.snapshot()
	if len(got) != 1 || got[0].Reason != knowledgeattemptaudit.ReasonServiceUnavailable {
		t.Fatalf("attempts=%+v", got)
	}
}

func TestKnowledgeValidateHandlerAppFailuresAreClosedAndReleasePermit(
	t *testing.T,
) {
	tests := []struct {
		name   string
		result KnowledgeAppCatalogResult
		err    error
	}{
		{name: "catalog error", err: errors.New("private app storage detail")},
		{name: "incomplete snapshot", result: KnowledgeAppCatalogResult{
			AppIDs: []string{knowledgeHTTPAppID}, Complete: false,
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			harness := newKnowledgePersistenceHarness(t, nil)
			harness.apps.result = test.result
			harness.apps.err = test.err
			attempts := &knowledgeValidationAttemptLog{}
			_, err := harness.handler.validateKnowledgeObject(
				knowledgeValidationDirectRequest(
					t,
					func(_ context.Context, definition knowledgeattemptaudit.Definition) error {
						return attempts.append(definition)
					},
				),
				knowledgeValidationCreateRequest(knowledgeHTTPDefinition(
					opensplunk.SharingScope_SHARING_SCOPE_PRIVATE,
				)),
			)
			if status := knowledgeValidationHTTPStatus(t, err); status != http.StatusServiceUnavailable {
				t.Fatalf("status=%d, want %d", status, http.StatusServiceUnavailable)
			}
			if strings.Contains(err.Error(), "private app storage detail") ||
				harness.apps.callCount() != 1 ||
				len(harness.handler.serializationGate) != 0 {
				t.Fatalf(
					"error=%v calls=%d gate=%d",
					err,
					harness.apps.callCount(),
					len(harness.handler.serializationGate),
				)
			}
			got := attempts.snapshot()
			if len(got) != 1 || got[0].Reason != knowledgeattemptaudit.ReasonServiceUnavailable {
				t.Fatalf("attempts=%+v", got)
			}
		})
	}
}

func TestKnowledgeValidateHandlerMissingCreateAppIsUniformAndJournaled(
	t *testing.T,
) {
	harness := newKnowledgePersistenceHarness(t, nil)
	before := readKnowledgeValidationPersistenceCounts(t, harness)
	definition := knowledgeHTTPDefinition(
		opensplunk.SharingScope_SHARING_SCOPE_PRIVATE,
	)
	definition.AppId = knowledgeHTTPOtherAppID
	request := knowledgeValidationDirectRequest(
		t,
		func(ctx context.Context, definition knowledgeattemptaudit.Definition) error {
			return harness.handler.appendKnowledgeRejectedAttempt(
				ctx,
				knowledgeBoundaryTenantID,
				definition,
			)
		},
	)
	_, err := harness.handler.validateKnowledgeObject(
		request,
		knowledgeValidationCreateRequest(definition),
	)
	if status := knowledgeValidationHTTPStatus(t, err); status != http.StatusNotFound {
		t.Fatalf("status=%d error=%v, want %d", status, err, http.StatusNotFound)
	}
	if len(harness.handler.serializationGate) != 0 {
		t.Fatalf("serialization gate=%d, want released", len(harness.handler.serializationGate))
	}
	after := readKnowledgeValidationPersistenceCounts(t, harness)
	if !reflect.DeepEqual(after, before) {
		t.Fatalf("missing-app validation changed catalog:\nbefore=%#v\nafter=%#v", before, after)
	}
	var action, reason string
	var appID, objectID sql.NullString
	if err := harness.database.SQLDB().QueryRowContext(t.Context(), `
		SELECT action, reason, app_id, knowledge_object_id
		FROM knowledge_attempt_audit_events WHERE tenant_id = ?`,
		knowledgeBoundaryTenantID,
	).Scan(&action, &reason, &appID, &objectID); err != nil {
		t.Fatalf("read missing-app validation attempt: %v", err)
	}
	if action != string(knowledgeattemptaudit.ActionValidate) ||
		reason != string(knowledgeattemptaudit.ReasonNotFoundOrForbidden) ||
		appID.Valid || objectID.Valid {
		t.Fatalf(
			"attempt action=%q reason=%q app=%v object=%v",
			action,
			reason,
			appID,
			objectID,
		)
	}
}

func TestKnowledgeValidateHandlerUpdateConflictJournalsExactObject(
	t *testing.T,
) {
	harness := newKnowledgePersistenceHarness(t, nil)
	created := &opensplunk.CreateKnowledgeObjectResponse{}
	knowledgePersistenceOK(
		t,
		harness.http,
		knowledgeObjectsCreatePath,
		&opensplunk.CreateKnowledgeObjectRequest{
			Definition: knowledgeHTTPDefinition(
				opensplunk.SharingScope_SHARING_SCOPE_PRIVATE,
			),
			InitialState:    opensplunk.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DRAFT,
			ClientRequestId: "validation-update-conflict-create-0001",
		},
		created,
	)
	before := readKnowledgeValidationPersistenceCounts(t, harness)
	objectID := created.GetKnowledgeObject().GetKnowledgeObjectId()
	expectedVersion := uint64(2)
	description := "candidate must not publish"
	validation := &opensplunk.ValidateKnowledgeObjectRequest{
		Definition:        &opensplunk.KnowledgeObjectDefinition{Description: &description},
		KnowledgeObjectId: &objectID,
		ExpectedVersion:   &expectedVersion,
		UpdateMask:        &fieldmaskpb.FieldMask{Paths: []string{"description"}},
		Intent:            opensplunk.KnowledgeValidationIntent_KNOWLEDGE_VALIDATION_INTENT_INACTIVE_STORAGE,
	}
	request := knowledgeValidationDirectRequest(
		t,
		func(ctx context.Context, definition knowledgeattemptaudit.Definition) error {
			return harness.handler.appendKnowledgeRejectedAttempt(
				ctx,
				knowledgeBoundaryTenantID,
				definition,
			)
		},
	)
	_, err := harness.handler.validateKnowledgeObject(request, validation)
	if status := knowledgeValidationHTTPStatus(t, err); status != http.StatusConflict {
		t.Fatalf("status=%d error=%v, want %d", status, err, http.StatusConflict)
	}
	if len(harness.handler.serializationGate) != 0 {
		t.Fatalf("serialization gate=%d, want released", len(harness.handler.serializationGate))
	}
	after := readKnowledgeValidationPersistenceCounts(t, harness)
	if !reflect.DeepEqual(after, before) {
		t.Fatalf("version-conflict validation changed catalog:\nbefore=%#v\nafter=%#v", before, after)
	}
	var action, reason, appID, persistedObjectID, objectType, sharingScope string
	var version int64
	if err := harness.database.SQLDB().QueryRowContext(t.Context(), `
		SELECT action, reason, app_id, knowledge_object_id,
		       object_type, object_version, sharing_scope
		FROM knowledge_attempt_audit_events WHERE tenant_id = ?`,
		knowledgeBoundaryTenantID,
	).Scan(
		&action,
		&reason,
		&appID,
		&persistedObjectID,
		&objectType,
		&version,
		&sharingScope,
	); err != nil {
		t.Fatalf("read update-conflict validation attempt: %v", err)
	}
	if action != string(knowledgeattemptaudit.ActionValidate) ||
		reason != string(knowledgeattemptaudit.ReasonVersionConflict) ||
		appID != knowledgeHTTPAppID || persistedObjectID != objectID ||
		objectType != string(knowledgeattemptaudit.ObjectTypeFieldAlias) ||
		version != 1 ||
		sharingScope != string(knowledgeattemptaudit.SharingScopePrivate) {
		t.Fatalf(
			"attempt action=%q reason=%q app=%q object=%q type=%q version=%d scope=%q",
			action,
			reason,
			appID,
			persistedObjectID,
			objectType,
			version,
			sharingScope,
		)
	}
}

func TestKnowledgeValidationScopeCloneIsIndependent(t *testing.T) {
	scopes := knowledgeScopes{
		read: knowledgecatalog.ReadScope{
			TenantID: "tenant", OwnerID: "owner",
			ReadableAppIDs: []string{"app-one", "app-two"},
		},
		write: knowledgecatalog.WriteScope{
			TenantID: "tenant", OwnerID: "owner",
			WritableAppIDs: []string{"app-one", "app-two"},
		},
		apps: []string{"app-one", "app-two"},
	}
	cloned := cloneKnowledgeValidationScope(scopes)
	cloned.Read.ReadableAppIDs[0] = "read-mutated"
	cloned.Write.WritableAppIDs[1] = "write-mutated"
	if !slices.Equal(scopes.read.ReadableAppIDs, []string{"app-one", "app-two"}) ||
		!slices.Equal(scopes.write.WritableAppIDs, []string{"app-one", "app-two"}) ||
		cloned.Write.WritableAppIDs[0] != "app-one" ||
		cloned.Read.ReadableAppIDs[1] != "app-two" {
		t.Fatalf("source=%+v cloned=%+v", scopes, cloned)
	}
}

func TestKnowledgeValidationBindingUsesExactASCIINormalization(t *testing.T) {
	ascii := knowledgeHTTPDefinition(opensplunk.SharingScope_SHARING_SCOPE_PRIVATE)
	ascii.AppId = " \t" + knowledgeHTTPAppID + "\r\n"
	if got := detachKnowledgeValidationRequestAuthority(
		knowledgeValidationCreateRequest(ascii),
	).appID; got != knowledgeHTTPAppID {
		t.Fatalf("ASCII-normalized app=%q, want %q", got, knowledgeHTTPAppID)
	}

	nonASCII := knowledgeHTTPDefinition(opensplunk.SharingScope_SHARING_SCOPE_PRIVATE)
	nonASCII.AppId = "\u00a0" + knowledgeHTTPAppID + "\u00a0"
	if got := detachKnowledgeValidationRequestAuthority(
		knowledgeValidationCreateRequest(nonASCII),
	).appID; got != nonASCII.AppId {
		t.Fatalf("non-ASCII-whitespace app=%q, want exact %q", got, nonASCII.AppId)
	}
}

type forgedKnowledgeValidationSentinel struct{ target error }

func (forgedKnowledgeValidationSentinel) Error() string { return "forged validation sentinel" }

func (forged forgedKnowledgeValidationSentinel) Is(target error) bool {
	return target == forged.target
}

type uncomparableKnowledgeValidationError []byte

func (uncomparableKnowledgeValidationError) Error() string {
	return "uncomparable validation error"
}

func definitiveKnowledgeValidationError(err error) error {
	return knowledgecatalog.WithErrorDisposition(
		err,
		knowledgecatalog.ErrorDispositionDefinitiveRejection,
	)
}

func TestKnowledgeValidationErrorSanitizerIsClosed(t *testing.T) {
	createInactive := knowledgeValidationRequestAuthority{
		intent: opensplunk.KnowledgeValidationIntent_KNOWLEDGE_VALIDATION_INTENT_INACTIVE_STORAGE,
	}
	createActive := knowledgeValidationRequestAuthority{
		intent: opensplunk.KnowledgeValidationIntent_KNOWLEDGE_VALIDATION_INTENT_ACTIVE_PUBLICATION,
	}
	updateInactive := createInactive
	updateInactive.update = true

	tests := []struct {
		name      string
		err       error
		authority knowledgeValidationRequestAuthority
		wantSafe  bool
	}{
		{"create not found", definitiveKnowledgeValidationError(control.ErrNotFound), createInactive, true},
		{"update not found", definitiveKnowledgeValidationError(control.ErrNotFound), updateInactive, true},
		{"update version conflict", definitiveKnowledgeValidationError(control.ErrVersionConflict), updateInactive, true},
		{"create version conflict impossible", definitiveKnowledgeValidationError(control.ErrVersionConflict), createInactive, false},
		{"invalid argument", definitiveKnowledgeValidationError(fmt.Errorf("envelope: %w", control.ErrInvalidArgument)), createInactive, true},
		{"active dependency conflict", definitiveKnowledgeValidationError(control.ErrDependencyConflict), createActive, true},
		{"inactive dependency conflict impossible", definitiveKnowledgeValidationError(control.ErrDependencyConflict), createInactive, false},
		{"capacity", definitiveKnowledgeValidationError(control.ErrCapacityExceeded), createInactive, true},
		{"canceled", definitiveKnowledgeValidationError(context.Canceled), createInactive, true},
		{"deadline", definitiveKnowledgeValidationError(context.DeadlineExceeded), createInactive, true},
		{"exact oversized response join", definitiveKnowledgeValidationError(errors.Join(
			control.ErrCapacityExceeded,
			knowledgevalidation.ErrResponseTooLarge,
		)), createInactive, true},
		{"missing disposition", control.ErrNotFound, createInactive, false},
		{"indeterminate disposition", knowledgecatalog.WithErrorDisposition(
			control.ErrNotFound,
			knowledgecatalog.ErrorDispositionIndeterminate,
		), createInactive, false},
		{"arbitrary not found join", definitiveKnowledgeValidationError(errors.Join(
			control.ErrNotFound,
			errors.New("private rollback detail"),
		)), updateInactive, false},
		{"capacity rollback join", definitiveKnowledgeValidationError(errors.Join(
			control.ErrCapacityExceeded,
			errors.New("roll back knowledge validation: private storage detail"),
		)), createInactive, false},
		{"forged Is method", definitiveKnowledgeValidationError(
			forgedKnowledgeValidationSentinel{target: control.ErrNotFound},
		), createInactive, false},
		{"uncomparable error", definitiveKnowledgeValidationError(
			uncomparableKnowledgeValidationError{1},
		), createInactive, false},
		{"service invariant", definitiveKnowledgeValidationError(
			knowledgevalidation.ErrInvariant,
		), createInactive, false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := sanitizeKnowledgeValidationError(test.err, test.authority)
			if exactKnowledgeValidationError(got, test.err) != test.wantSafe {
				t.Fatalf("sanitized=%v original=%v wantSafe=%t", got, test.err, test.wantSafe)
			}
		})
	}
}

func TestKnowledgeValidationCreateDependencyContextExceptionIsNarrow(
	t *testing.T,
) {
	scopes := knowledgeScopes{
		apps: []string{knowledgeHTTPAppID},
	}
	create := knowledgeRejectionBinding{
		kind:   knowledgeRejectionBindingCreate,
		scopes: scopes,
		appID:  knowledgeHTTPAppID,
	}
	update := knowledgeRejectionBinding{
		kind:     knowledgeRejectionBindingObject,
		scopes:   scopes,
		targetID: "ko-validation-target",
	}
	appOnly := &knowledgecatalog.AuthorizedContext{AppID: knowledgeHTTPAppID}
	object := &knowledgecatalog.AuthorizedContext{
		AppID: knowledgeHTTPAppID,
		Object: &knowledgecatalog.AuthorizedObject{
			KnowledgeObjectID: "ko-validation-target",
			ObjectType:        knowledgecatalog.ObjectTypeFieldAlias,
			Version:           1,
			SharingScope:      knowledgecatalog.SharingScopePrivate,
		},
	}
	requestForAction := func(action knowledgeattemptaudit.Action) *http.Request {
		request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, knowledgeObjectsValidatePath, nil)
		return request.WithContext(context.WithValue(
			request.Context(),
			knowledgeAttemptStateContextKey{},
			&knowledgeAttemptState{action: action},
		))
	}
	validate := requestForAction(knowledgeattemptaudit.ActionValidate)
	if !knowledgeRejectionContextSatisfies(
		validate,
		knowledgeattemptaudit.ReasonForbiddenDependency,
		appOnly,
		[]knowledgeRejectionBinding{create},
	) {
		t.Fatal("Validate create exact app-only dependency context was rejected")
	}
	mismatched := &knowledgecatalog.AuthorizedContext{AppID: knowledgeHTTPOtherAppID}
	checks := []struct {
		name       string
		request    *http.Request
		authorized *knowledgecatalog.AuthorizedContext
		binding    knowledgeRejectionBinding
	}{
		{"missing app context", validate, nil, create},
		{"mismatched app", validate, mismatched, create},
		{"create object context", validate, object, create},
		{"update app-only context", validate, appOnly, update},
		{"ordinary create action", requestForAction(knowledgeattemptaudit.ActionCreate), appOnly, create},
	}
	for _, check := range checks {
		t.Run(check.name, func(t *testing.T) {
			if knowledgeRejectionContextSatisfies(
				check.request,
				knowledgeattemptaudit.ReasonForbiddenDependency,
				check.authorized,
				[]knowledgeRejectionBinding{check.binding},
			) {
				t.Fatal("forged or out-of-scope dependency context was accepted")
			}
		})
	}
	if !knowledgeRejectionContextSatisfies(
		validate,
		knowledgeattemptaudit.ReasonForbiddenDependency,
		object,
		[]knowledgeRejectionBinding{update},
	) {
		t.Fatal("Validate update exact object dependency context was rejected")
	}
}

func TestKnowledgeValidationPathIsRegisteredInExactManagementBoundary(t *testing.T) {
	appender := &knowledgeBoundaryAppender{}
	handler, httpHandler := newKnowledgeHTTPHandler(
		t,
		auth.BrowserRoleAdministrator,
		&knowledgeHTTPCatalog{},
		&knowledgeHTTPWriter{},
		knowledgeHTTPApps(),
		appender,
	)
	if routes := handler.knowledgeManagementRoutes(router.NoAuth); len(routes) != 9 {
		t.Fatalf("management routes=%d, want exactly nine", len(routes))
	}
	request := httptest.NewRequestWithContext(
		t.Context(),
		http.MethodPost,
		knowledgeObjectsValidatePath,
		bytes.NewReader(nil),
	)
	if action, protected := knowledgeAttemptFallbackAction(request); !protected ||
		action != knowledgeattemptaudit.ActionValidate {
		t.Fatalf("Validate fallback action=%q protected=%t", action, protected)
	}
	response := knowledgeHTTPPost(
		t,
		httpHandler,
		knowledgeObjectsValidatePath,
		knowledgeValidationCreateRequest(knowledgeHTTPDefinition(
			opensplunk.SharingScope_SHARING_SCOPE_PRIVATE,
		)),
	)
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("Validate with nonconcrete test Writer status=%d body=%q", response.Code, response.Body.String())
	}
	if got := appender.snapshot(); len(got) != 1 ||
		got[0].definition.Action != knowledgeattemptaudit.ActionValidate ||
		got[0].definition.Reason != knowledgeattemptaudit.ReasonServiceUnavailable {
		t.Fatalf("Validate attempts=%+v", got)
	}
}
