package knowledgecatalog

import (
	"context"
	"errors"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/audit"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/knowledgevalidation"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"gorm.io/gorm"
)

type validationPanicAppender struct{}

func (validationPanicAppender) AppendInTransaction(
	context.Context,
	*gorm.DB,
	string,
	audit.SuccessfulEvent,
) (audit.Event, error) {
	panic("validation called the mutation audit appender")
}

func newWriterValidationHarness(
	t *testing.T,
	withIndex bool,
) (*control.DB, *Writer, ValidationScope) {
	t.Helper()
	database, _ := newCatalogTestStore(t)
	if withIndex {
		createPublicationTransitionTestIndex(t, database, "main")
	}
	writer, err := NewWriter(database, validationPanicAppender{}, WriterOptions{
		Clock: func() time.Time {
			panic("validation called the mutation clock")
		},
		IDGenerator: func() (string, error) {
			panic("validation called the mutation ID generator")
		},
	})
	if err != nil {
		t.Fatalf("NewWriter(validation): %v", err)
	}
	writer.hook = func(context.Context, writerHookEvent) error {
		panic("validation called the mutation hook")
	}
	writer.commit = func(*gorm.DB) error {
		panic("validation called the mutation commit seam")
	}
	return database, writer, ValidationScope{
		Read: ReadScope{
			TenantID:       testTenant,
			OwnerID:        testOwner,
			ReadableAppIDs: []string{testApp},
		},
		Write: WriteScope{
			TenantID:       testTenant,
			OwnerID:        testOwner,
			WritableAppIDs: []string{testApp},
		},
	}
}

func validationAliasDefinition(name string, withIndex bool) *opensplunkv1.KnowledgeObjectDefinition {
	definition := aliasDefinition(testApp, name, SharingScopePrivate, nil, "validation-host-*")
	if withIndex {
		definition = writerActiveRouteDefinition(definition, "main")
	}
	return definition
}

func validationCreateRequest(
	definition *opensplunkv1.KnowledgeObjectDefinition,
	intent opensplunkv1.KnowledgeValidationIntent,
) *opensplunkv1.ValidateKnowledgeObjectRequest {
	return &opensplunkv1.ValidateKnowledgeObjectRequest{
		Definition: definition,
		Intent:     intent,
	}
}

func validationUpdateRequest(
	objectID string,
	version uint64,
	definition *opensplunkv1.KnowledgeObjectDefinition,
	paths ...string,
) *opensplunkv1.ValidateKnowledgeObjectRequest {
	return &opensplunkv1.ValidateKnowledgeObjectRequest{
		Definition:        definition,
		KnowledgeObjectId: stringPointer(objectID),
		ExpectedVersion:   uint64Pointer(version),
		UpdateMask:        &fieldmaskpb.FieldMask{Paths: paths},
		Intent:            opensplunkv1.KnowledgeValidationIntent_KNOWLEDGE_VALIDATION_INTENT_INACTIVE_STORAGE,
	}
}

func TestValidateKnowledgeObjectRequestPresenceEnvelope(t *testing.T) {
	validDefinition := &opensplunkv1.KnowledgeObjectDefinition{}
	validCreate := validationCreateRequest(
		validDefinition,
		opensplunkv1.KnowledgeValidationIntent_KNOWLEDGE_VALIDATION_INTENT_INACTIVE_STORAGE,
	)
	validUpdate := validationUpdateRequest(
		"ko-validation-envelope",
		1,
		validDefinition,
		"name",
	)

	if err := ValidateKnowledgeObjectRequest(validCreate); err != nil {
		t.Fatalf("valid create envelope: %v", err)
	}
	if err := ValidateKnowledgeObjectRequest(validUpdate); err != nil {
		t.Fatalf("valid update envelope: %v", err)
	}

	tests := []struct {
		name    string
		request *opensplunkv1.ValidateKnowledgeObjectRequest
	}{
		{name: "nil request"},
		{name: "missing definition", request: &opensplunkv1.ValidateKnowledgeObjectRequest{
			Intent: opensplunkv1.KnowledgeValidationIntent_KNOWLEDGE_VALIDATION_INTENT_INACTIVE_STORAGE,
		}},
		{name: "unspecified intent", request: &opensplunkv1.ValidateKnowledgeObjectRequest{Definition: validDefinition}},
		{name: "unknown intent", request: &opensplunkv1.ValidateKnowledgeObjectRequest{
			Definition: validDefinition, Intent: opensplunkv1.KnowledgeValidationIntent(99),
		}},
		{name: "create expected version present", request: &opensplunkv1.ValidateKnowledgeObjectRequest{
			Definition: validDefinition, ExpectedVersion: uint64Pointer(0),
			Intent: opensplunkv1.KnowledgeValidationIntent_KNOWLEDGE_VALIDATION_INTENT_INACTIVE_STORAGE,
		}},
		{name: "create update mask present", request: &opensplunkv1.ValidateKnowledgeObjectRequest{
			Definition: validDefinition, UpdateMask: &fieldmaskpb.FieldMask{},
			Intent: opensplunkv1.KnowledgeValidationIntent_KNOWLEDGE_VALIDATION_INTENT_INACTIVE_STORAGE,
		}},
		{name: "update empty present ID", request: validationUpdateRequest("", 1, validDefinition, "name")},
		{name: "update expected version absent", request: &opensplunkv1.ValidateKnowledgeObjectRequest{
			Definition: validDefinition, KnowledgeObjectId: stringPointer("ko-validation-envelope"),
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"name"}},
			Intent:     opensplunkv1.KnowledgeValidationIntent_KNOWLEDGE_VALIDATION_INTENT_INACTIVE_STORAGE,
		}},
		{name: "update expected version zero", request: validationUpdateRequest("ko-validation-envelope", 0, validDefinition, "name")},
		{name: "update expected version overflow", request: validationUpdateRequest("ko-validation-envelope", math.MaxInt64+1, validDefinition, "name")},
		{name: "update mask empty", request: validationUpdateRequest("ko-validation-envelope", 1, validDefinition)},
		{name: "update mask noncanonical", request: validationUpdateRequest("ko-validation-envelope", 1, validDefinition, "name", "app_id")},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := ValidateKnowledgeObjectRequest(test.request); !errors.Is(err, control.ErrInvalidArgument) {
				t.Fatalf("error = %v, want ErrInvalidArgument", err)
			}
		})
	}

	unknownEnvelope := proto.Clone(validCreate).(*opensplunkv1.ValidateKnowledgeObjectRequest)
	unknownEnvelope.ProtoReflect().SetUnknown(protowire.AppendVarint(
		protowire.AppendTag(nil, 19000, protowire.VarintType),
		1,
	))
	if err := ValidateKnowledgeObjectRequest(unknownEnvelope); !errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf("unknown request envelope error = %v, want ErrInvalidArgument", err)
	}
	unknownMask := proto.Clone(validUpdate).(*opensplunkv1.ValidateKnowledgeObjectRequest)
	unknownMask.GetUpdateMask().ProtoReflect().SetUnknown(protowire.AppendVarint(
		protowire.AppendTag(nil, 19000, protowire.VarintType),
		1,
	))
	if err := ValidateKnowledgeObjectRequest(unknownMask); !errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf("unknown update mask error = %v, want ErrInvalidArgument", err)
	}
	unknownCandidate := proto.Clone(validCreate).(*opensplunkv1.ValidateKnowledgeObjectRequest)
	unknownCandidate.GetDefinition().ProtoReflect().SetUnknown(protowire.AppendVarint(
		protowire.AppendTag(nil, 19000, protowire.VarintType),
		1,
	))
	if err := ValidateKnowledgeObjectRequest(unknownCandidate); err != nil {
		t.Fatalf("candidate unknown field became envelope error: %v", err)
	}
}

func TestWriterValidateInactiveIsDetachedRevisionZeroAndSideEffectFree(t *testing.T) {
	database, writer, scope := newWriterValidationHarness(t, false)
	request := validationCreateRequest(
		validationAliasDefinition("validation-inactive", false),
		opensplunkv1.KnowledgeValidationIntent_KNOWLEDGE_VALIDATION_INTENT_INACTIVE_STORAGE,
	)
	submitted := proto.Clone(request).(*opensplunkv1.ValidateKnowledgeObjectRequest)
	before := readValidationPersistenceSnapshot(t, database)

	sealed, err := writer.Validate(t.Context(), scope, request)
	if err != nil {
		t.Fatalf("Validate(INACTIVE create): %v", err)
	}
	if !proto.Equal(request, submitted) {
		t.Fatalf("Validate mutated request: got %v want %v", request, submitted)
	}
	response, err := sealed.Proto(t.Context())
	if err != nil {
		t.Fatalf("sealed.Proto(): %v", err)
	}
	if response.GetTenantCatalogRevision() != 0 || !response.GetResult().GetValid() ||
		response.GetResult().GetNormalizedDefinition().GetName() != "validation-inactive" ||
		response.GetResult().GetResources() == nil || len(response.GetResult().GetDependencies()) != 0 {
		t.Fatalf("INACTIVE response = %v", response)
	}
	request.Definition.Name = "caller-mutated"
	response.Result.NormalizedDefinition.Name = "response-mutated"
	again, err := sealed.Proto(t.Context())
	if err != nil || again.GetResult().GetNormalizedDefinition().GetName() != "validation-inactive" {
		t.Fatalf("sealed response retained caller mutation: %v, %v", again, err)
	}
	after := readValidationPersistenceSnapshot(t, database)
	if !reflect.DeepEqual(after, before) {
		t.Fatalf("validation changed persistence:\nbefore=%#v\nafter=%#v", before, after)
	}
}

func TestWriterValidateGateFollowsEnvelopeAndPrecedesCandidateWork(t *testing.T) {
	_, writer, scope := newWriterValidationHarness(t, false)
	if !writer.validationGate.TryAcquire() {
		t.Fatal("failed to reserve validation gate")
	}
	defer writer.validationGate.Release()

	malformed := &opensplunkv1.ValidateKnowledgeObjectRequest{
		Intent: opensplunkv1.KnowledgeValidationIntent_KNOWLEDGE_VALIDATION_INTENT_INACTIVE_STORAGE,
	}
	if _, err := writer.Validate(t.Context(), scope, malformed); !errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf("malformed envelope under full gate error = %v, want ErrInvalidArgument", err)
	}
	candidate := validationCreateRequest(
		validationAliasDefinition("validation-gated", false),
		opensplunkv1.KnowledgeValidationIntent_KNOWLEDGE_VALIDATION_INTENT_INACTIVE_STORAGE,
	)
	candidate.Definition.ProtoReflect().SetUnknown(protowire.AppendVarint(
		protowire.AppendTag(nil, 19000, protowire.VarintType),
		1,
	))
	if _, err := writer.Validate(t.Context(), scope, candidate); !errors.Is(err, control.ErrCapacityExceeded) {
		t.Fatalf("candidate under full gate error = %v, want ErrCapacityExceeded", err)
	} else if disposition, ok := ErrorDispositionFromError(err); !ok || disposition != ErrorDispositionDefinitiveRejection {
		t.Fatalf("gate error disposition = %v, %t", disposition, ok)
	}
}

func TestWriterValidateUpdateAppliesExactMaskWithoutMutationOnlyChecks(t *testing.T) {
	database, writer, scope := newWriterValidationHarness(t, false)
	insertFixtureObject(t, database, fixtureObject{
		id:    "ko-validation-update",
		owner: testOwner,
		versions: []fixtureVersion{{
			definition: validationAliasDefinition("validation-current", false),
			state:      StateDraft,
			mutation:   "create",
			timestamp:  10,
		}},
	})
	before := readValidationPersistenceSnapshot(t, database)
	request := validationUpdateRequest(
		"ko-validation-update",
		1,
		&opensplunkv1.KnowledgeObjectDefinition{Name: "validation-applied"},
		"name",
	)
	sealed, err := writer.Validate(t.Context(), scope, request)
	if err != nil {
		t.Fatalf("Validate(INACTIVE update): %v", err)
	}
	response, err := sealed.Proto(t.Context())
	if err != nil || !response.GetResult().GetValid() ||
		response.GetResult().GetNormalizedDefinition().GetName() != "validation-applied" ||
		response.GetResult().GetNormalizedDefinition().GetAppId() != testApp {
		t.Fatalf("masked validation = %v, %v", response, err)
	}

	noOp := validationUpdateRequest(
		"ko-validation-update",
		1,
		&opensplunkv1.KnowledgeObjectDefinition{Name: "validation-current"},
		"name",
	)
	if _, err := writer.Validate(t.Context(), scope, noOp); err != nil {
		t.Fatalf("valid masked no-op was rejected: %v", err)
	}
	wrongVersion := validationUpdateRequest(
		"ko-validation-update",
		2,
		&opensplunkv1.KnowledgeObjectDefinition{Name: "validation-applied"},
		"name",
	)
	if _, err := writer.Validate(t.Context(), scope, wrongVersion); !errors.Is(err, control.ErrVersionConflict) {
		t.Fatalf("wrong-version validation error = %v, want ErrVersionConflict", err)
	} else if authorized, ok := AuthorizedContextFromError(err); !ok || authorized.Object == nil ||
		authorized.Object.KnowledgeObjectID != "ko-validation-update" {
		t.Fatalf("wrong-version authorized context = %#v, %t", authorized, ok)
	}
	if after := readValidationPersistenceSnapshot(t, database); !reflect.DeepEqual(after, before) {
		t.Fatalf("update validation changed persistence:\nbefore=%#v\nafter=%#v", before, after)
	}
}

func TestWriterValidateActiveProjectsOnlyAuthorizedCandidateDependencies(t *testing.T) {
	database, writer, scope := newWriterValidationHarness(t, true)
	insertFixtureObject(t, database, fixtureObject{
		id:    "ko-validation-target",
		owner: testOwner,
		versions: []fixtureVersion{{
			definition: writerActiveRouteDefinition(dependencyExtractionDefinition(
				testApp,
				"validation-target",
				SharingScopeApp,
				nil,
				"validation-host-*",
				dependencyFixtureInputField,
			), "main"),
			state:     StateActive,
			mutation:  "create",
			timestamp: 10,
		}},
	})
	candidate := writerActiveRouteDefinition(dependencyAliasDefinition(
		testApp,
		"validation-active-candidate",
		SharingScopePrivate,
		nil,
		"validation-host-*",
		dependencyFixtureInputField,
		"validation_alias",
	), "main")
	request := validationCreateRequest(
		candidate,
		opensplunkv1.KnowledgeValidationIntent_KNOWLEDGE_VALIDATION_INTENT_ACTIVE_PUBLICATION,
	)
	before := readValidationPersistenceSnapshot(t, database)
	sealed, err := writer.Validate(t.Context(), scope, request)
	if err != nil {
		t.Fatalf("Validate(ACTIVE with dependency): %v", err)
	}
	response, err := sealed.Proto(t.Context())
	if err != nil {
		t.Fatalf("ACTIVE dependency sealed response: %v", err)
	}
	dependencies := response.GetResult().GetDependencies()
	if !response.GetResult().GetValid() || len(dependencies) != 1 ||
		dependencies[0].GetTarget().GetKnowledgeObjectId() != "ko-validation-target" ||
		dependencies[0].GetTarget().GetVersion() != 1 ||
		dependencies[0].GetRole() != opensplunkv1.KnowledgeDependencyRole_KNOWLEDGE_DEPENDENCY_ROLE_FIELD_INPUT {
		t.Fatalf("ACTIVE dependency response = %v", response)
	}

	hiddenScope := scope
	hiddenScope.Read.ReadableAppIDs = []string{testAppTwo}
	hidden, err := writer.Validate(t.Context(), hiddenScope, request)
	if err != nil {
		t.Fatalf("Validate(ACTIVE hidden dependency): %v", err)
	}
	hiddenResponse, err := hidden.Proto(t.Context())
	if err != nil {
		t.Fatalf("hidden dependency sealed response: %v", err)
	}
	if hiddenResponse.GetResult().GetValid() ||
		len(hiddenResponse.GetResult().GetDependencies()) != 0 ||
		len(hiddenResponse.GetResult().GetDiagnostics()) != 1 ||
		hiddenResponse.GetResult().GetDiagnostics()[0].GetDiagnostic().GetCode() != "KNOWLEDGE_DEPENDENCY_UNAVAILABLE" ||
		strings.Contains(hiddenResponse.String(), "ko-validation-target") {
		t.Fatalf("hidden dependency response = %v", hiddenResponse)
	}
	preparation, err := knowledgevalidation.PrepareActive(t.Context(), candidate)
	if err != nil {
		t.Fatalf("prepare target-absence candidate: %v", err)
	}
	activeCandidate, ok := preparation.Candidate()
	if !ok {
		t.Fatal("target-absence fixture did not prepare an ACTIVE candidate")
	}
	missingResult, candidateInvalid, err := validationTransitionConflictResult(
		t.Context(),
		activeCandidate,
		publicationActiveValidationCohortDependencyTargetAbsent,
	)
	if err != nil || !candidateInvalid {
		t.Fatalf("map target-absence conflict = invalid %t, err %v", candidateInvalid, err)
	}
	missingProjection, err := missingResult.Proto(t.Context())
	if err != nil || !proto.Equal(missingProjection, hiddenResponse.GetResult()) {
		t.Fatalf(
			"transition target absence and unauthorized dependency results differ:\nmissing=%v\nhidden=%v\nerr=%v",
			missingProjection,
			hiddenResponse,
			err,
		)
	}
	if after := readValidationPersistenceSnapshot(t, database); !reflect.DeepEqual(after, before) {
		t.Fatalf("ACTIVE validation changed persistence:\nbefore=%#v\nafter=%#v", before, after)
	}
}

func TestWriterValidateRejectsOpaqueAppliedCurrentBodyOutOfBand(t *testing.T) {
	database, writer, scope := newWriterValidationHarness(t, false)
	insertFixtureObject(t, database, fixtureObject{
		id:    "ko-validation-opaque-target",
		owner: testOwner,
		versions: []fixtureVersion{{
			definition: dependencyExtractionDefinition(
				testApp,
				"validation-opaque-target",
				SharingScopePrivate,
				nil,
				"validation-opaque-*",
				dependencyFixtureInputField,
			),
			state:     StateActive,
			mutation:  "create",
			timestamp: 10,
		}},
	})
	fixture := newIntegrationFutureDefinition(t, "validation-opaque-current", 0)
	seedWriterInactiveOpaquePublication(
		t,
		database,
		"ko-validation-opaque-current",
		fixture,
		[]fixtureDependency{{targetObjectID: "ko-validation-opaque-target", targetVersion: 1}},
		20,
	)
	description := "candidate metadata"
	request := validationUpdateRequest(
		"ko-validation-opaque-current",
		1,
		&opensplunkv1.KnowledgeObjectDefinition{Description: &description},
		"description",
	)
	if _, err := writer.Validate(t.Context(), scope, request); !errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf("opaque applied body error = %v, want out-of-band ErrInvalidArgument", err)
	}
}

func TestWriterValidateCancellationIsDefinitiveAndDoesNotEnterSQLite(t *testing.T) {
	_, writer, scope := newWriterValidationHarness(t, false)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := writer.Validate(ctx, scope, validationCreateRequest(
		validationAliasDefinition("validation-canceled", false),
		opensplunkv1.KnowledgeValidationIntent_KNOWLEDGE_VALIDATION_INTENT_INACTIVE_STORAGE,
	))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled validation error = %v, want context.Canceled", err)
	}
	if disposition, ok := ErrorDispositionFromError(err); !ok ||
		disposition != ErrorDispositionDefinitiveRejection {
		t.Fatalf("canceled validation disposition = %v, %t", disposition, ok)
	}
}

type validationPersistenceSnapshot struct {
	catalog catalogState
	counts  map[string]int64
}

func readValidationPersistenceSnapshot(
	t *testing.T,
	database *control.DB,
) validationPersistenceSnapshot {
	t.Helper()
	state, err := readCatalogState(database.GORMDB(), testTenant)
	if err != nil {
		t.Fatalf("read validation catalog state: %v", err)
	}
	tables := []string{
		"knowledge_catalog_tenants",
		"knowledge_catalog_revision_heads",
		"knowledge_projection_tenant_ledgers",
		"knowledge_definition_blobs",
		"knowledge_objects",
		"knowledge_object_versions",
		"knowledge_object_version_lifecycle",
		"knowledge_object_dependencies",
		"knowledge_object_dependency_seals",
		"knowledge_object_list_projections",
		"knowledge_object_list_selector_patterns",
		"knowledge_object_list_projection_seals",
		"knowledge_object_list_order_keys",
		"knowledge_mutation_commit_authorities",
		"knowledge_mutation_idempotency",
		"knowledge_recovery_audit",
		"audit_events",
	}
	counts := make(map[string]int64, len(tables))
	for _, table := range tables {
		var count int64
		if err := database.GORMDB().Table(table).Count(&count).Error; err != nil {
			t.Fatalf("count %s: %v", table, err)
		}
		counts[table] = count
	}
	return validationPersistenceSnapshot{catalog: state, counts: counts}
}
