package server

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/auth"
	"github.com/Suhaibinator/open-splunk/internal/knowledgeattemptaudit"
	"github.com/Suhaibinator/open-splunk/internal/knowledgecatalog"
	"github.com/Suhaibinator/open-splunk/internal/knowledgevalidation"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

func knowledgeValidationRawRequest(
	t *testing.T,
	body io.Reader,
) *http.Request {
	t.Helper()
	request := httptest.NewRequestWithContext(
		t.Context(),
		http.MethodPost,
		knowledgeObjectsValidatePath,
		body,
	)
	request.Host = "example.com"
	request.Header.Set("Origin", "http://example.com")
	request.Header.Set("Sec-Fetch-Site", "same-origin")
	request.Header.Set("Authorization", "Bearer "+knowledgeBoundaryToken)
	request.Header.Set("Content-Type", "application/x-protobuf")
	return request
}

func knowledgeValidationRawPost(
	t *testing.T,
	handler http.Handler,
	payload []byte,
) *httptest.ResponseRecorder {
	t.Helper()
	response := httptest.NewRecorder()
	handler.ServeHTTP(
		response,
		knowledgeValidationRawRequest(t, bytes.NewReader(payload)),
	)
	return response
}

func knowledgeValidationHTTPResponse(
	t *testing.T,
	response *httptest.ResponseRecorder,
) *opensplunk.ValidateKnowledgeObjectResponse {
	t.Helper()
	if response.Code != http.StatusOK {
		t.Fatalf("Validate status=%d body=%q", response.Code, response.Body.String())
	}
	if contentType := response.Header().Get("Content-Type"); contentType != "application/x-protobuf" {
		t.Fatalf("Validate content type=%q", contentType)
	}
	decoded := &opensplunk.ValidateKnowledgeObjectResponse{}
	if err := proto.Unmarshal(response.Body.Bytes(), decoded); err != nil {
		t.Fatalf("decode Validate response: %v", err)
	}
	return decoded
}

func newKnowledgeValidationHTTPHarness(
	t *testing.T,
) (*knowledgePersistenceHarness, *apiHandler, http.Handler, *knowledgeBoundaryAppender) {
	t.Helper()
	harness := newKnowledgePersistenceHarness(t, nil)
	attempts := &knowledgeBoundaryAppender{}
	handler, httpHandler := newKnowledgeHTTPHandler(
		t,
		auth.BrowserRoleAdministrator,
		harness.catalog,
		harness.writer,
		harness.apps,
		attempts,
	)
	return harness, handler, httpHandler, attempts
}

func TestKnowledgeValidationHTTPWritesExactSealAndBoundsMillionEntryCandidates(
	t *testing.T,
) {
	harness, _, httpHandler, attempts := newKnowledgeValidationHTTPHarness(t)
	definition := knowledgeHTTPDefinition(
		opensplunk.SharingScope_SHARING_SCOPE_PRIVATE,
	)
	result, err := knowledgevalidation.BuildInactive(t.Context(), definition)
	if err != nil {
		t.Fatalf("BuildInactive: %v", err)
	}
	sealed, err := knowledgevalidation.SealValidateResponse(t.Context(), result, 0)
	if err != nil {
		t.Fatalf("SealValidateResponse: %v", err)
	}
	response := knowledgeHTTPPost(
		t,
		httpHandler,
		knowledgeObjectsValidatePath,
		knowledgeValidationCreateRequest(definition),
	)
	decoded := knowledgeValidationHTTPResponse(t, response)
	if !decoded.GetResult().GetValid() ||
		!bytes.Equal(response.Body.Bytes(), sealed.DeterministicBytes()) {
		t.Fatalf(
			"valid response=%v exact seal=%t",
			decoded,
			bytes.Equal(response.Body.Bytes(), sealed.DeterministicBytes()),
		)
	}

	selectorPayload := bytes.Repeat(validateTestBytesField(1, nil), 1_000_000)
	definitionPayload := append(
		validateTestBytesField(5, selectorPayload),
		validateTestBytesField(11, nil)...,
	)
	selectedWire := validateTestBytesField(1, definitionPayload)
	selectedWire = append(selectedWire, validateTestVarintField(
		5,
		uint64(opensplunk.KnowledgeValidationIntent_KNOWLEDGE_VALIDATION_INTENT_INACTIVE_STORAGE),
	)...)
	selected := knowledgeValidationHTTPResponse(
		t,
		knowledgeValidationRawPost(t, httpHandler, selectedWire),
	)
	selectedViolations := selected.GetResult().GetFieldViolations()
	if selected.GetResult().GetValid() || len(selectedViolations) != 1 ||
		selectedViolations[0].GetFieldPath() != "selector.index_patterns" ||
		selectedViolations[0].GetCode() != "KNOWLEDGE_DEFINITION_RESOURCE_LIMIT" {
		t.Fatalf("million selected result=%v", selected)
	}

	created := &opensplunk.CreateKnowledgeObjectResponse{}
	knowledgePersistenceOK(
		t,
		httpHandler,
		knowledgeObjectsCreatePath,
		&opensplunk.CreateKnowledgeObjectRequest{
			Definition: knowledgeHTTPDefinition(
				opensplunk.SharingScope_SHARING_SCOPE_PRIVATE,
			),
			InitialState:    opensplunk.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DRAFT,
			ClientRequestId: "validation-http-million-create-0001",
		},
		created,
	)
	before := readKnowledgeValidationPersistenceCounts(t, harness)
	updateDefinition := append(
		validateTestBytesField(2, []byte("million-unselected-update")),
		validateTestBytesField(5, selectorPayload)...,
	)
	unselectedWire := validateTestBytesField(1, updateDefinition)
	unselectedWire = append(unselectedWire, validateTestBytesField(
		2,
		[]byte(created.GetKnowledgeObject().GetKnowledgeObjectId()),
	)...)
	unselectedWire = append(unselectedWire, validateTestVarintField(3, 1)...)
	unselectedWire = append(unselectedWire, validateTestBytesField(
		4,
		validateTestMarshal(t, &fieldmaskpb.FieldMask{Paths: []string{"name"}}),
	)...)
	unselectedWire = append(unselectedWire, validateTestVarintField(
		5,
		uint64(opensplunk.KnowledgeValidationIntent_KNOWLEDGE_VALIDATION_INTENT_INACTIVE_STORAGE),
	)...)
	unselected := knowledgeValidationHTTPResponse(
		t,
		knowledgeValidationRawPost(t, httpHandler, unselectedWire),
	)
	if !unselected.GetResult().GetValid() ||
		unselected.GetResult().GetNormalizedDefinition().GetName() !=
			"million-unselected-update" {
		t.Fatalf("million unselected result=%v", unselected)
	}
	after := readKnowledgeValidationPersistenceCounts(t, harness)
	if before != after || len(attempts.snapshot()) != 0 {
		t.Fatalf(
			"validation changed persistence or journaled success: before=%+v after=%+v attempts=%+v",
			before,
			after,
			attempts.snapshot(),
		)
	}
}

func TestKnowledgeValidationHTTPPreservesEnvelopeAndCandidateUnknownSemantics(
	t *testing.T,
) {
	_, _, httpHandler, attempts := newKnowledgeValidationHTTPHarness(t)

	outer := knowledgeValidationCreateRequest(knowledgeHTTPDefinition(
		opensplunk.SharingScope_SHARING_SCOPE_PRIVATE,
	))
	outer.ProtoReflect().SetUnknown(validateTestVarintField(100, 1))
	outerResponse := knowledgeHTTPPost(
		t,
		httpHandler,
		knowledgeObjectsValidatePath,
		outer,
	)
	if outerResponse.Code != http.StatusBadRequest {
		t.Fatalf("outer unknown status=%d body=%q", outerResponse.Code, outerResponse.Body.String())
	}

	objectID := "ko-mask-envelope"
	version := uint64(1)
	mask := &fieldmaskpb.FieldMask{Paths: []string{"name"}}
	mask.ProtoReflect().SetUnknown(validateTestVarintField(101, 1))
	maskResponse := knowledgeHTTPPost(
		t,
		httpHandler,
		knowledgeObjectsValidatePath,
		&opensplunk.ValidateKnowledgeObjectRequest{
			Definition:        &opensplunk.KnowledgeObjectDefinition{Name: "mask-unknown"},
			KnowledgeObjectId: &objectID,
			ExpectedVersion:   &version,
			UpdateMask:        mask,
			Intent:            opensplunk.KnowledgeValidationIntent_KNOWLEDGE_VALIDATION_INTENT_INACTIVE_STORAGE,
		},
	)
	if maskResponse.Code != http.StatusBadRequest {
		t.Fatalf("mask unknown status=%d body=%q", maskResponse.Code, maskResponse.Body.String())
	}

	selectedDefinition := knowledgeHTTPDefinition(
		opensplunk.SharingScope_SHARING_SCOPE_PRIVATE,
	)
	selectedDefinition.GetFieldAlias().ProtoReflect().SetUnknown(
		validateTestVarintField(102, 1),
	)
	selected := knowledgeValidationHTTPResponse(
		t,
		knowledgeHTTPPost(
			t,
			httpHandler,
			knowledgeObjectsValidatePath,
			knowledgeValidationCreateRequest(selectedDefinition),
		),
	)
	selectedViolations := selected.GetResult().GetFieldViolations()
	if selected.GetResult().GetValid() || len(selectedViolations) != 1 ||
		selectedViolations[0].GetFieldPath() != "field_alias" ||
		selectedViolations[0].GetCode() != "KNOWLEDGE_DEFINITION_UNKNOWN_FIELD" {
		t.Fatalf("selected nested unknown result=%v", selected)
	}

	created := &opensplunk.CreateKnowledgeObjectResponse{}
	knowledgePersistenceOK(
		t,
		httpHandler,
		knowledgeObjectsCreatePath,
		&opensplunk.CreateKnowledgeObjectRequest{
			Definition: knowledgeHTTPDefinition(
				opensplunk.SharingScope_SHARING_SCOPE_PRIVATE,
			),
			InitialState:    opensplunk.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DRAFT,
			ClientRequestId: "validation-http-unknown-create-0001",
		},
		created,
	)
	objectID = created.GetKnowledgeObject().GetKnowledgeObjectId()
	unselectedDefinition := &opensplunk.KnowledgeObjectDefinition{
		Name: "unselected-nested-unknown",
		Body: &opensplunk.KnowledgeObjectDefinition_FieldAlias{
			FieldAlias: &opensplunk.FieldAliasDefinition{},
		},
	}
	unselectedDefinition.GetFieldAlias().ProtoReflect().SetUnknown(
		validateTestVarintField(103, 1),
	)
	unselected := knowledgeValidationHTTPResponse(
		t,
		knowledgeHTTPPost(
			t,
			httpHandler,
			knowledgeObjectsValidatePath,
			&opensplunk.ValidateKnowledgeObjectRequest{
				Definition:        unselectedDefinition,
				KnowledgeObjectId: &objectID,
				ExpectedVersion:   &version,
				UpdateMask:        &fieldmaskpb.FieldMask{Paths: []string{"name"}},
				Intent:            opensplunk.KnowledgeValidationIntent_KNOWLEDGE_VALIDATION_INTENT_INACTIVE_STORAGE,
			},
		),
	)
	if !unselected.GetResult().GetValid() ||
		unselected.GetResult().GetNormalizedDefinition().GetName() !=
			"unselected-nested-unknown" {
		t.Fatalf("unselected nested unknown result=%v", unselected)
	}

	gotAttempts := attempts.snapshot()
	if len(gotAttempts) != 2 {
		t.Fatalf("unknown attempts=%+v", gotAttempts)
	}
	for _, attempt := range gotAttempts {
		if attempt.definition.Action != knowledgeattemptaudit.ActionValidate ||
			attempt.definition.Reason != knowledgeattemptaudit.ReasonInvalidDefinition ||
			attempt.definition.AuthorizedContext != nil {
			t.Fatalf("unknown attempt=%+v", attempt)
		}
	}
}

func TestKnowledgeValidationHTTPWriterAdmissionAndAuthenticationOrdering(
	t *testing.T,
) {
	harness, handler, httpHandler, attempts := newKnowledgeValidationHTTPHarness(t)
	gate, err := harness.database.SharedAdmissionGate(
		"knowledge-catalog-validation",
		knowledgecatalog.MaximumConcurrentValidations,
	)
	if err != nil || !gate.TryAcquire() {
		t.Fatalf("reserve Writer validation gate: %v", err)
	}
	defer gate.Release()

	selectorPayload := bytes.Repeat(validateTestBytesField(1, nil), 1_000_000)
	definitionPayload := append(
		validateTestBytesField(5, selectorPayload),
		validateTestBytesField(11, nil)...,
	)
	wire := validateTestBytesField(1, definitionPayload)
	wire = append(wire, validateTestVarintField(
		5,
		uint64(opensplunk.KnowledgeValidationIntent_KNOWLEDGE_VALIDATION_INTENT_INACTIVE_STORAGE),
	)...)
	fullGate := knowledgeValidationRawPost(t, httpHandler, wire)
	if fullGate.Code != http.StatusTooManyRequests ||
		len(handler.serializationGate) != 0 || harness.apps.callCount() != 1 {
		t.Fatalf(
			"full Writer gate status=%d body=%q serialization=%d apps=%d",
			fullGate.Code,
			fullGate.Body.String(),
			len(handler.serializationGate),
			harness.apps.callCount(),
		)
	}
	gotAttempts := attempts.snapshot()
	if len(gotAttempts) != 1 ||
		gotAttempts[0].definition.Action != knowledgeattemptaudit.ActionValidate ||
		gotAttempts[0].definition.Reason != knowledgeattemptaudit.ReasonResourceLimit ||
		gotAttempts[0].definition.AuthorizedContext != nil {
		t.Fatalf("full Writer gate attempts=%+v", gotAttempts)
	}

	unauthorizedBody := newKnowledgeBoundaryObservedBody("unread validation secret", nil)
	unauthorizedRequest := knowledgeValidationRawRequest(t, unauthorizedBody)
	unauthorizedRequest.Header.Del("Authorization")
	unauthorized := httptest.NewRecorder()
	httpHandler.ServeHTTP(unauthorized, unauthorizedRequest)
	if unauthorized.Code != http.StatusUnauthorized || unauthorizedBody.reads() != 0 ||
		len(attempts.snapshot()) != 1 {
		t.Fatalf(
			"unauthorized status=%d body=%q reads=%d attempts=%+v",
			unauthorized.Code,
			unauthorized.Body.String(),
			unauthorizedBody.reads(),
			attempts.snapshot(),
		)
	}

	userAttempts := &knowledgeBoundaryAppender{}
	_, userHTTP := newKnowledgeHTTPHandler(
		t,
		auth.BrowserRoleUser,
		harness.catalog,
		harness.writer,
		harness.apps,
		userAttempts,
	)
	userBody := newKnowledgeBoundaryObservedBody("unread user validation secret", nil)
	userResponse := httptest.NewRecorder()
	userHTTP.ServeHTTP(
		userResponse,
		knowledgeValidationRawRequest(t, userBody),
	)
	userJournal := userAttempts.snapshot()
	if userResponse.Code != http.StatusForbidden || userBody.reads() != 0 ||
		len(userJournal) != 1 ||
		userJournal[0].definition.Action != knowledgeattemptaudit.ActionValidate ||
		userJournal[0].definition.Reason != knowledgeattemptaudit.ReasonNotAdministrator {
		t.Fatalf(
			"user status=%d body=%q reads=%d attempts=%+v",
			userResponse.Code,
			userResponse.Body.String(),
			userBody.reads(),
			userJournal,
		)
	}
}

func TestKnowledgeValidationHTTPRawRequestLimitIsExactAndJournaled(t *testing.T) {
	_, _, httpHandler, attempts := newKnowledgeValidationHTTPHarness(t)
	limit := int(maximumKnowledgeMutationRequestBytes)
	tag := protowire.AppendTag(nil, 100, protowire.BytesType)
	payloadLength := limit - len(tag) - protowire.SizeVarint(uint64(limit))
	exact := append([]byte(nil), tag...)
	exact = protowire.AppendBytes(exact, make([]byte, payloadLength))
	if len(exact) != limit {
		t.Fatalf("exact request bytes=%d, want %d", len(exact), limit)
	}
	exactResponse := knowledgeValidationRawPost(t, httpHandler, exact)
	overResponse := knowledgeValidationRawPost(t, httpHandler, append(exact, 0))
	if exactResponse.Code != http.StatusBadRequest ||
		overResponse.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf(
			"exact/over status=%d/%d bodies=%q/%q",
			exactResponse.Code,
			overResponse.Code,
			exactResponse.Body.String(),
			overResponse.Body.String(),
		)
	}
	gotAttempts := attempts.snapshot()
	if len(gotAttempts) != 2 ||
		gotAttempts[0].definition.Action != knowledgeattemptaudit.ActionValidate ||
		gotAttempts[0].definition.Reason != knowledgeattemptaudit.ReasonInvalidDefinition ||
		gotAttempts[1].definition.Action != knowledgeattemptaudit.ActionValidate ||
		gotAttempts[1].definition.Reason != knowledgeattemptaudit.ReasonResourceLimit {
		t.Fatalf("raw-limit attempts=%+v", gotAttempts)
	}
}

func TestKnowledgeValidationHTTPRouteBypassesOuterAdministratorPolicy(
	t *testing.T,
) {
	writer := newReadyKnowledgeWriter(t)
	attempts := &knowledgeBoundaryAppender{}
	config := knowledgeConfigBase(t)
	config.BrowserAuthenticator = &knowledgeBoundaryAuthenticator{
		principal: knowledgeBoundaryPrincipal(t, auth.BrowserRoleUser),
	}
	config.KnowledgeCatalog = &knowledgeHTTPCatalog{}
	config.KnowledgeWriter = writer
	config.KnowledgeApps = knowledgeHTTPApps()
	config.KnowledgeAttempts = attempts
	handler, err := NewHandler(config)
	if err != nil {
		t.Fatalf("NewHandler: %v", err)
	}
	body := newKnowledgeBoundaryObservedBody("unread production validation secret", nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, knowledgeValidationRawRequest(t, body))
	journal := attempts.snapshot()
	if response.Code != http.StatusForbidden || body.reads() != 0 ||
		len(journal) != 1 ||
		journal[0].definition.Action != knowledgeattemptaudit.ActionValidate ||
		journal[0].definition.Reason != knowledgeattemptaudit.ReasonNotAdministrator {
		t.Fatalf(
			"production user status=%d body=%q reads=%d attempts=%+v",
			response.Code,
			response.Body.String(),
			body.reads(),
			journal,
		)
	}
}
