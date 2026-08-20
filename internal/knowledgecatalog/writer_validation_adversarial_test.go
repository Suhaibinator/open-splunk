package knowledgecatalog

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/knowledge"
	"github.com/Suhaibinator/open-splunk/internal/knowledgedefinition"
	"github.com/Suhaibinator/open-splunk/internal/knowledgevalidation"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"gorm.io/gorm"
)

func validationOverCardinalitySelector(name string) *opensplunk.KnowledgeObjectDefinition {
	return validationOverCardinalitySelectorCount(
		name,
		knowledge.MaximumSelectorPatternsPerDimension+1,
	)
}

func validationOverCardinalitySelectorCount(
	name string,
	count int,
) *opensplunk.KnowledgeObjectDefinition {
	definition := validationAliasDefinition(name, false)
	definition.Selector.HostPatterns = make(
		[]*opensplunk.KnowledgeSelectorPattern,
		count,
	)
	// Exercise both an allocated empty entry and typed nil entries. Neither can
	// be traversed or cloned before the length witness closes the candidate.
	definition.Selector.HostPatterns[0] = &opensplunk.KnowledgeSelectorPattern{}
	return definition
}

func validationOverCardinalityExtraction(name string) *opensplunk.KnowledgeObjectDefinition {
	return validationOverCardinalityExtractionCount(
		name,
		knowledgedefinition.MaximumFieldExtractionOutputs+1,
	)
}

func validationOverCardinalityExtractionCount(
	name string,
	count int,
) *opensplunk.KnowledgeObjectDefinition {
	definition := validationAliasDefinition(name, false)
	definition.Body = &opensplunk.KnowledgeObjectDefinition_FieldExtraction{
		FieldExtraction: &opensplunk.FieldExtractionDefinition{
			Extraction: &opensplunk.FieldExtractionDefinition_Regex{
				Regex: &opensplunk.RegexFieldExtractionDefinition{
					OutputFields: make([]string, count),
				},
			},
		},
	}
	return definition
}

func requireValidationDefinitionResourceLimit(
	t *testing.T,
	sealed knowledgevalidation.SealedValidateResponse,
	objectType opensplunk.KnowledgeObjectType,
	fieldPath string,
) {
	t.Helper()
	response, err := sealed.Proto(t.Context())
	if err != nil {
		t.Fatalf("sealed resource-limit response: %v", err)
	}
	violations := response.GetResult().GetFieldViolations()
	if response.GetResult().GetValid() || response.GetResult().GetObjectType() != objectType ||
		len(violations) != 1 || violations[0].GetFieldPath() != fieldPath ||
		violations[0].GetCode() != "KNOWLEDGE_DEFINITION_RESOURCE_LIMIT" ||
		response.GetResult().GetNormalizedDefinition() != nil ||
		len(response.GetResult().GetDependencies()) != 0 {
		t.Fatalf("resource-limit response = %v", response)
	}
}

func TestWriterValidateActiveCreateIsAlphaInvariantAcrossFreshCollisionFamily(t *testing.T) {
	validateWithCollision := func(t *testing.T, collisionID string) []byte {
		t.Helper()
		database, writer, scope := newWriterValidationHarness(t, true)
		insertFixtureObject(t, database, fixtureObject{
			id:    collisionID,
			owner: testOwner,
			versions: []fixtureVersion{{
				definition: validationAliasDefinition("validation-inactive-collision", false),
				state:      StateDraft,
				mutation:   "create",
				timestamp:  10,
			}},
		})
		candidate := writerActiveRouteDefinition(dependencyExtractionDefinition(
			testApp,
			"validation-alpha-candidate",
			SharingScopePrivate,
			nil,
			"validation-alpha-*",
			"validation_alpha_output",
		), "main")
		sealed, err := writer.Validate(t.Context(), scope, validationCreateRequest(
			candidate,
			opensplunk.KnowledgeValidationIntent_KNOWLEDGE_VALIDATION_INTENT_ACTIVE_PUBLICATION,
		))
		if err != nil {
			t.Fatalf("Validate(ACTIVE collision %q): %v", collisionID, err)
		}
		response, err := sealed.Proto(t.Context())
		if err != nil || !response.GetResult().GetValid() || response.GetTenantCatalogRevision() != 1 ||
			len(response.GetResult().GetDependencies()) != 0 {
			t.Fatalf("ACTIVE collision response = %v, %v", response, err)
		}
		return sealed.DeterministicBytes()
	}

	first := validateWithCollision(t, "knowledge-validation-candidate-0000")
	second := validateWithCollision(t, "knowledge-validation-candidate-0001")
	if !bytes.Equal(first, second) {
		t.Fatalf("fresh-ID alpha rename changed the complete response:\nfirst=%x\nsecond=%x", first, second)
	}
}

func TestWriterValidateActiveUpdateCoversDraftDisabledAndActiveCurrent(t *testing.T) {
	database, writer, scope := newWriterValidationHarness(t, true)
	insertFixtureObject(t, database, fixtureObject{
		id:    "ko-validation-lifecycle-target",
		owner: testOwner,
		versions: []fixtureVersion{{
			definition: writerActiveRouteDefinition(dependencyExtractionDefinition(
				testApp,
				"validation-lifecycle-target",
				SharingScopePrivate,
				nil,
				"validation-lifecycle-*",
				dependencyFixtureInputField,
			), "main"),
			state:     StateActive,
			mutation:  "create",
			timestamp: 10,
		}},
	})
	alias := func(name string) *opensplunk.KnowledgeObjectDefinition {
		return writerActiveRouteDefinition(dependencyAliasDefinition(
			testApp,
			name,
			SharingScopePrivate,
			nil,
			"validation-lifecycle-*",
			dependencyFixtureInputField,
			name+"_alias",
		), "main")
	}
	insertFixtureObject(t, database, fixtureObject{
		id: "ko-validation-current-draft", owner: testOwner,
		versions: []fixtureVersion{{
			definition: alias("validation-current-draft"), state: StateDraft,
			mutation: "create", timestamp: 20,
		}},
	})
	disabledDefinition := alias("validation-current-disabled")
	insertFixtureObject(t, database, fixtureObject{
		id: "ko-validation-current-disabled", owner: testOwner,
		versions: []fixtureVersion{
			{
				definition: disabledDefinition, state: StateActive, mutation: "create", timestamp: 30,
				dependencies: []fixtureDependency{{
					targetObjectID: "ko-validation-lifecycle-target", targetVersion: 1,
				}},
			},
			{
				definition: proto.Clone(disabledDefinition).(*opensplunk.KnowledgeObjectDefinition),
				state:      StateDisabled, mutation: "disable", timestamp: 40,
				dependencies: []fixtureDependency{{
					targetObjectID: "ko-validation-lifecycle-target", targetVersion: 1,
				}},
			},
		},
	})
	insertFixtureObject(t, database, fixtureObject{
		id: "ko-validation-current-active", owner: testOwner,
		versions: []fixtureVersion{{
			definition: alias("validation-current-active"), state: StateActive,
			mutation: "create", timestamp: 50,
			dependencies: []fixtureDependency{{
				targetObjectID: "ko-validation-lifecycle-target", targetVersion: 1,
			}},
		}},
	})
	before := readValidationPersistenceSnapshot(t, database)
	revision := uint64(before.catalog.revision)
	for _, test := range []struct {
		name     string
		objectID string
		version  uint64
	}{
		{name: "draft", objectID: "ko-validation-current-draft", version: 1},
		{name: "disabled", objectID: "ko-validation-current-disabled", version: 2},
		{name: "active no-op", objectID: "ko-validation-current-active", version: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			currentName := strings.TrimPrefix(test.objectID, "ko-")
			request := validationUpdateRequest(
				test.objectID,
				test.version,
				&opensplunk.KnowledgeObjectDefinition{Name: currentName},
				"name",
			)
			request.Intent = opensplunk.KnowledgeValidationIntent_KNOWLEDGE_VALIDATION_INTENT_ACTIVE_PUBLICATION
			sealed, err := writer.Validate(t.Context(), scope, request)
			if err != nil {
				t.Fatalf("Validate(ACTIVE %s): %v", test.name, err)
			}
			response, err := sealed.Proto(t.Context())
			if err != nil {
				t.Fatalf("sealed ACTIVE %s: %v", test.name, err)
			}
			dependencies := response.GetResult().GetDependencies()
			if !response.GetResult().GetValid() || response.GetTenantCatalogRevision() != revision ||
				len(dependencies) != 1 ||
				dependencies[0].GetTarget().GetKnowledgeObjectId() != "ko-validation-lifecycle-target" ||
				dependencies[0].GetTarget().GetVersion() != 1 {
				t.Fatalf("ACTIVE %s response = %v", test.name, response)
			}
		})
	}
	if after := readValidationPersistenceSnapshot(t, database); !reflect.DeepEqual(after, before) {
		t.Fatalf("ACTIVE lifecycle validation changed persistence:\nbefore=%#v\nafter=%#v", before, after)
	}
}

func TestWriterValidateCardinalityAdmissionStaysInBandWithoutCloneAmplification(t *testing.T) {
	_, writer, scope := newWriterValidationHarness(t, false)
	combined := validationOverCardinalityExtraction("validation-combined-order")
	combined.Selector = validationOverCardinalitySelector("ignored-combined-selector").Selector
	for _, test := range []struct {
		name       string
		definition *opensplunk.KnowledgeObjectDefinition
		objectType opensplunk.KnowledgeObjectType
		fieldPath  string
		intent     opensplunk.KnowledgeValidationIntent
	}{
		{
			name:       "inactive selector with empty and nil entries",
			definition: validationOverCardinalitySelector("validation-selector-inactive"),
			objectType: opensplunk.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_FIELD_ALIAS,
			fieldPath:  "selector.host_patterns",
			intent:     opensplunk.KnowledgeValidationIntent_KNOWLEDGE_VALIDATION_INTENT_INACTIVE_STORAGE,
		},
		{
			name:       "active selector with empty and nil entries",
			definition: validationOverCardinalitySelector("validation-selector-active"),
			objectType: opensplunk.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_FIELD_ALIAS,
			fieldPath:  "selector.host_patterns",
			intent:     opensplunk.KnowledgeValidationIntent_KNOWLEDGE_VALIDATION_INTENT_ACTIVE_PUBLICATION,
		},
		{
			name:       "inactive empty regex outputs",
			definition: validationOverCardinalityExtraction("validation-outputs-inactive"),
			objectType: opensplunk.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_FIELD_EXTRACTION,
			fieldPath:  "field_extraction.regex.output_fields",
			intent:     opensplunk.KnowledgeValidationIntent_KNOWLEDGE_VALIDATION_INTENT_INACTIVE_STORAGE,
		},
		{
			name:       "active empty regex outputs",
			definition: validationOverCardinalityExtraction("validation-outputs-active"),
			objectType: opensplunk.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_FIELD_EXTRACTION,
			fieldPath:  "field_extraction.regex.output_fields",
			intent:     opensplunk.KnowledgeValidationIntent_KNOWLEDGE_VALIDATION_INTENT_ACTIVE_PUBLICATION,
		},
		{
			name:       "selector precedes regex outputs",
			definition: combined,
			objectType: opensplunk.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_FIELD_EXTRACTION,
			fieldPath:  "selector.host_patterns",
			intent:     opensplunk.KnowledgeValidationIntent_KNOWLEDGE_VALIDATION_INTENT_INACTIVE_STORAGE,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			sealed, err := writer.Validate(t.Context(), scope, validationCreateRequest(
				test.definition,
				test.intent,
			))
			if err != nil {
				t.Fatalf("Validate(over-cardinality create): %v", err)
			}
			requireValidationDefinitionResourceLimit(
				t,
				sealed,
				test.objectType,
				test.fieldPath,
			)
		})
	}
}

func TestWriterValidateMillionEntryCardinalityCollapsesToBoundedWitness(t *testing.T) {
	_, writer, scope := newWriterValidationHarness(t, false)
	const hostileEntries = 1 << 20

	t.Run("selector nil messages", func(t *testing.T) {
		request := validationCreateRequest(
			validationOverCardinalitySelectorCount("validation-million-selector", hostileEntries),
			opensplunk.KnowledgeValidationIntent_KNOWLEDGE_VALIDATION_INTENT_INACTIVE_STORAGE,
		)
		prepared, err := normalizeValidationRequest(request)
		if err != nil {
			t.Fatalf("normalize million-entry selector: %v", err)
		}
		if got := len(prepared.definition.GetSelector().GetHostPatterns()); got != knowledge.MaximumSelectorPatternsPerDimension+1 {
			t.Fatalf("selector witness entries = %d, want %d", got, knowledge.MaximumSelectorPatternsPerDimension+1)
		}
		sealed, err := writer.Validate(t.Context(), scope, request)
		if err != nil {
			t.Fatalf("Validate(million-entry selector): %v", err)
		}
		requireValidationDefinitionResourceLimit(
			t,
			sealed,
			opensplunk.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_FIELD_ALIAS,
			"selector.host_patterns",
		)
	})

	t.Run("empty output strings", func(t *testing.T) {
		request := validationCreateRequest(
			validationOverCardinalityExtractionCount("validation-million-outputs", hostileEntries),
			opensplunk.KnowledgeValidationIntent_KNOWLEDGE_VALIDATION_INTENT_INACTIVE_STORAGE,
		)
		prepared, err := normalizeValidationRequest(request)
		if err != nil {
			t.Fatalf("normalize million-entry outputs: %v", err)
		}
		if got := len(prepared.definition.GetFieldExtraction().GetRegex().GetOutputFields()); got != knowledgedefinition.MaximumFieldExtractionOutputs+1 {
			t.Fatalf("output witness entries = %d, want %d", got, knowledgedefinition.MaximumFieldExtractionOutputs+1)
		}
		sealed, err := writer.Validate(t.Context(), scope, request)
		if err != nil {
			t.Fatalf("Validate(million-entry outputs): %v", err)
		}
		requireValidationDefinitionResourceLimit(
			t,
			sealed,
			opensplunk.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_FIELD_EXTRACTION,
			"field_extraction.regex.output_fields",
		)
	})
}

func TestWriterValidateUpdateCardinalityHonorsRootMaskAndBodyApplicability(t *testing.T) {
	database, writer, scope := newWriterValidationHarness(t, false)
	insertFixtureObject(t, database, fixtureObject{
		id: "ko-validation-cardinality-alias", owner: testOwner,
		versions: []fixtureVersion{{
			definition: validationAliasDefinition("validation-cardinality-alias", false),
			state:      StateDraft, mutation: "create", timestamp: 10,
		}},
	})
	insertFixtureObject(t, database, fixtureObject{
		id: "ko-validation-cardinality-extraction", owner: testOwner,
		versions: []fixtureVersion{{
			definition: dependencyExtractionDefinition(
				testApp,
				"validation-cardinality-extraction",
				SharingScopePrivate,
				nil,
				"validation-cardinality-*",
				"validation_cardinality_output",
			),
			state: StateDraft, mutation: "create", timestamp: 20,
		}},
	})

	selectorRequest := validationUpdateRequest(
		"ko-validation-cardinality-alias",
		1,
		validationOverCardinalitySelector("ignored-by-mask"),
		"selector",
	)
	sealed, err := writer.Validate(t.Context(), scope, selectorRequest)
	if err != nil {
		t.Fatalf("selected over-cardinality selector: %v", err)
	}
	requireValidationDefinitionResourceLimit(
		t,
		sealed,
		opensplunk.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_FIELD_ALIAS,
		"selector.host_patterns",
	)

	wrongVersion := proto.Clone(selectorRequest).(*opensplunk.ValidateKnowledgeObjectRequest)
	wrongVersion.ExpectedVersion = new(uint64(2))
	if _, err := writer.Validate(t.Context(), scope, wrongVersion); !errors.Is(err, control.ErrVersionConflict) {
		t.Fatalf("over-cardinality wrong-version error = %v, want ErrVersionConflict", err)
	}

	extractionRequest := validationUpdateRequest(
		"ko-validation-cardinality-extraction",
		1,
		validationOverCardinalityExtraction("ignored-by-mask"),
		"field_extraction",
	)
	sealed, err = writer.Validate(t.Context(), scope, extractionRequest)
	if err != nil {
		t.Fatalf("selected over-cardinality outputs: %v", err)
	}
	requireValidationDefinitionResourceLimit(
		t,
		sealed,
		opensplunk.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_FIELD_EXTRACTION,
		"field_extraction.regex.output_fields",
	)

	typedNil := validationOverCardinalitySelector("ignored-by-mask")
	typedNil.Body = &opensplunk.KnowledgeObjectDefinition_FieldAlias{}
	typedNilRequest := validationUpdateRequest(
		"ko-validation-cardinality-alias",
		1,
		typedNil,
		"field_alias",
		"selector",
	)
	if _, err := writer.Validate(t.Context(), scope, typedNilRequest); !errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf("typed-nil selected body error = %v, want ErrInvalidArgument", err)
	}

	wrongBodyRequest := validationUpdateRequest(
		"ko-validation-cardinality-alias",
		1,
		validationOverCardinalityExtraction("ignored-by-mask"),
		"field_extraction",
	)
	if _, err := writer.Validate(t.Context(), scope, wrongBodyRequest); !errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf("different selected body error = %v, want ErrInvalidArgument", err)
	}

	nestedUnknown := &opensplunk.KnowledgeObjectDefinition{
		Selector: &opensplunk.KnowledgeSelector{
			HostPatterns: []*opensplunk.KnowledgeSelectorPattern{{Value: "validation-unknown-*"}},
		},
	}
	nestedUnknown.GetSelector().GetHostPatterns()[0].ProtoReflect().SetUnknown(protowire.AppendVarint(
		protowire.AppendTag(nil, 19000, protowire.VarintType),
		1,
	))
	unknownRequest := validationUpdateRequest(
		"ko-validation-cardinality-alias",
		1,
		nestedUnknown,
		"selector",
	)
	unknownSealed, err := writer.Validate(t.Context(), scope, unknownRequest)
	if err != nil {
		t.Fatalf("selected nested unknown: %v", err)
	}
	unknownResponse, err := unknownSealed.Proto(t.Context())
	if err != nil {
		t.Fatalf("selected nested unknown response: %v", err)
	}
	violations := unknownResponse.GetResult().GetFieldViolations()
	if unknownResponse.GetResult().GetValid() || len(violations) != 1 ||
		violations[0].GetFieldPath() != "selector.host_patterns[0]" ||
		violations[0].GetCode() != "KNOWLEDGE_DEFINITION_UNKNOWN_FIELD" {
		t.Fatalf("selected nested unknown response = %v", unknownResponse)
	}
}

func TestWriterValidateSelectedViewOwnsByteAuthority(t *testing.T) {
	database, writer, scope := newWriterValidationHarness(t, false)
	insertFixtureObject(t, database, fixtureObject{
		id: "ko-validation-selected-view", owner: testOwner,
		versions: []fixtureVersion{{
			definition: validationAliasDefinition("validation-selected-view", false),
			state:      StateDraft, mutation: "create", timestamp: 10,
		}},
	})

	oversizedEnvelopeValue := strings.Repeat("x", maximumValidationRequestBytes+1)
	tooLargeCreate := validationCreateRequest(
		validationAliasDefinition("validation-selected-too-large", false),
		opensplunk.KnowledgeValidationIntent_KNOWLEDGE_VALIDATION_INTENT_INACTIVE_STORAGE,
	)
	tooLargeCreate.Definition.Description = &oversizedEnvelopeValue
	if err := ValidateKnowledgeObjectRequest(tooLargeCreate); err != nil {
		t.Fatalf("envelope-only helper inspected candidate bytes: %v", err)
	}
	if _, err := writer.Validate(t.Context(), scope, tooLargeCreate); !errors.Is(err, control.ErrCapacityExceeded) {
		t.Fatalf("selected request above hard byte bound = %v, want ErrCapacityExceeded", err)
	}

	selectedName := "validation-selected-view-updated"
	unselectedHuge := validationOverCardinalitySelectorCount("ignored-name", 1<<20)
	unselectedHuge.Name = selectedName
	unselectedHuge.Description = &oversizedEnvelopeValue
	unselectedHuge.Body = validationOverCardinalityExtractionCount("ignored-body", 1<<20).Body
	unselectedRequest := validationUpdateRequest(
		"ko-validation-selected-view",
		1,
		unselectedHuge,
		"name",
	)
	if err := ValidateKnowledgeObjectRequest(unselectedRequest); err != nil {
		t.Fatalf("envelope helper inspected unselected million-entry fields: %v", err)
	}
	sealed, err := writer.Validate(t.Context(), scope, unselectedRequest)
	if err != nil {
		t.Fatalf("unselected oversized fields affected update: %v", err)
	}
	response, err := sealed.Proto(t.Context())
	if err != nil || !response.GetResult().GetValid() ||
		response.GetResult().GetNormalizedDefinition().GetName() != selectedName {
		t.Fatalf("selected-view update response = %v, %v", response, err)
	}

	definitionLimitValue := strings.Repeat("x", knowledgedefinition.MaximumCanonicalBytes+1)
	definitionTooLarge := validationCreateRequest(
		validationAliasDefinition("validation-definition-too-large", false),
		opensplunk.KnowledgeValidationIntent_KNOWLEDGE_VALIDATION_INTENT_INACTIVE_STORAGE,
	)
	definitionTooLarge.Definition.Description = &definitionLimitValue
	sealed, err = writer.Validate(t.Context(), scope, definitionTooLarge)
	if err != nil {
		t.Fatalf("definition-only byte excess became envelope error: %v", err)
	}
	requireValidationDefinitionResourceLimit(
		t,
		sealed,
		opensplunk.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_FIELD_ALIAS,
		"",
	)
}

func TestValidateKnowledgeObjectRequestRejectsHugeMaskBeforeCandidateWalk(t *testing.T) {
	request := validationUpdateRequest(
		"ko-validation-huge-mask",
		1,
		validationOverCardinalitySelector("validation-huge-mask"),
		"name",
	)
	request.UpdateMask.Paths = make([]string, 1<<18)
	if err := ValidateKnowledgeObjectRequest(request); !errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf("huge update-mask error = %v, want ErrInvalidArgument", err)
	}
}

type validationRequestMutationContext struct {
	context.Context
	mutate func()
	once   sync.Once
	calls  atomic.Int64
}

func (ctx *validationRequestMutationContext) Err() error {
	ctx.calls.Add(1)
	ctx.once.Do(ctx.mutate)
	return ctx.Context.Err()
}

func TestWriterValidateDetachesBeforeCallerContextCallback(t *testing.T) {
	_, writer, scope := newWriterValidationHarness(t, false)
	request := validationCreateRequest(
		validationAliasDefinition("validation-context-detached", false),
		opensplunk.KnowledgeValidationIntent_KNOWLEDGE_VALIDATION_INTENT_INACTIVE_STORAGE,
	)
	ctx := &validationRequestMutationContext{
		Context: t.Context(),
		mutate: func() {
			request.Definition.Name = "caller-context-mutated"
			request.Definition.Selector = validationOverCardinalitySelector("ignored").Selector
		},
	}
	sealed, err := writer.Validate(ctx, scope, request)
	if err != nil {
		t.Fatalf("Validate(context mutation): %v", err)
	}
	response, err := sealed.Proto(t.Context())
	if err != nil || ctx.calls.Load() == 0 ||
		response.GetResult().GetNormalizedDefinition().GetName() != "validation-context-detached" {
		t.Fatalf(
			"post-detach context mutation response = %v, calls %d, err %v",
			response,
			ctx.calls.Load(),
			err,
		)
	}
}

func TestWriterValidateNilAndCanceledContextAdmissionPrecedence(t *testing.T) {
	_, writer, scope := newWriterValidationHarness(t, false)
	request := validationCreateRequest(
		validationOverCardinalitySelectorCount("validation-context-precedence", 1<<20),
		opensplunk.KnowledgeValidationIntent_KNOWLEDGE_VALIDATION_INTENT_INACTIVE_STORAGE,
	)
	if !writer.validationGate.TryAcquire() {
		t.Fatal("failed to reserve validation gate")
	}
	defer writer.validationGate.Release()
	var nilContext context.Context
	if _, err := writer.Validate(nilContext, scope, request); !errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf("nil-context error = %v, want ErrInvalidArgument", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := writer.Validate(ctx, scope, request); !errors.Is(err, control.ErrCapacityExceeded) ||
		errors.Is(err, context.Canceled) {
		t.Fatalf("saturated gate vs canceled context error = %v, want only ErrCapacityExceeded", err)
	}
}

func TestWriterValidateLocalInvalidityPrecedesAppAndInventoryAuthority(t *testing.T) {
	database, writer, scope := newWriterValidationHarness(t, true)
	archiveValidationApp(t, database, testApp)

	semanticInvalid := writerActiveRouteDefinition(dependencyCalculatedDefinition(
		testApp,
		"validation-semantic-invalid",
		SharingScopePrivate,
		nil,
		"validation-invalid-*",
		"source +",
		"validation_invalid_result",
	), "main")
	sealed, err := writer.Validate(t.Context(), scope, validationCreateRequest(
		semanticInvalid,
		opensplunk.KnowledgeValidationIntent_KNOWLEDGE_VALIDATION_INTENT_ACTIVE_PUBLICATION,
	))
	if err != nil {
		t.Fatalf("locally invalid ACTIVE candidate consulted archived app/inventory: %v", err)
	}
	semanticResponse, err := sealed.Proto(t.Context())
	if err != nil || semanticResponse.GetResult().GetValid() ||
		len(semanticResponse.GetResult().GetDiagnostics()) == 0 {
		t.Fatalf("semantic-invalid response = %v, %v", semanticResponse, err)
	}

	definitionInvalid := validationAliasDefinition("validation-definition-invalid", true)
	definitionInvalid.ProtoReflect().SetUnknown(protowire.AppendVarint(
		protowire.AppendTag(nil, 19000, protowire.VarintType),
		1,
	))
	sealed, err = writer.Validate(t.Context(), scope, validationCreateRequest(
		definitionInvalid,
		opensplunk.KnowledgeValidationIntent_KNOWLEDGE_VALIDATION_INTENT_ACTIVE_PUBLICATION,
	))
	if err != nil {
		t.Fatalf("definition-invalid ACTIVE candidate consulted archived app/inventory: %v", err)
	}
	definitionResponse, err := sealed.Proto(t.Context())
	if err != nil || definitionResponse.GetResult().GetValid() ||
		len(definitionResponse.GetResult().GetFieldViolations()) != 1 {
		t.Fatalf("definition-invalid response = %v, %v", definitionResponse, err)
	}

	inactiveArchived, err := writer.Validate(t.Context(), scope, validationCreateRequest(
		validationAliasDefinition("validation-archived-inactive", false),
		opensplunk.KnowledgeValidationIntent_KNOWLEDGE_VALIDATION_INTENT_INACTIVE_STORAGE,
	))
	if err != nil {
		t.Fatalf("locally valid INACTIVE candidate in archived app: %v", err)
	}
	inactiveResponse, err := inactiveArchived.Proto(t.Context())
	if err != nil || !inactiveResponse.GetResult().GetValid() {
		t.Fatalf("archived INACTIVE response = %v, %v", inactiveResponse, err)
	}
	if _, err := writer.Validate(t.Context(), scope, validationCreateRequest(
		validationAliasDefinition("validation-archived-active", true),
		opensplunk.KnowledgeValidationIntent_KNOWLEDGE_VALIDATION_INTENT_ACTIVE_PUBLICATION,
	)); !errors.Is(err, control.ErrNotFound) {
		t.Fatalf("locally valid ACTIVE archived-app error = %v, want ErrNotFound", err)
	}

	const missingApp = "app_000000000100000000099A"
	missingScope := scope
	missingScope.Write.WritableAppIDs = []string{missingApp}
	missingDefinition := validationAliasDefinition("validation-missing-app", false)
	missingDefinition.AppId = missingApp
	for _, intent := range []opensplunk.KnowledgeValidationIntent{
		opensplunk.KnowledgeValidationIntent_KNOWLEDGE_VALIDATION_INTENT_INACTIVE_STORAGE,
		opensplunk.KnowledgeValidationIntent_KNOWLEDGE_VALIDATION_INTENT_ACTIVE_PUBLICATION,
	} {
		if _, err := writer.Validate(t.Context(), missingScope, validationCreateRequest(
			missingDefinition,
			intent,
		)); !errors.Is(err, control.ErrNotFound) {
			t.Fatalf("missing-app intent %v error = %v, want ErrNotFound", intent, err)
		}
	}
}

func TestWriterValidateSharedGateAcrossConcreteWriters(t *testing.T) {
	database, first, scope := newWriterValidationHarness(t, false)
	second, err := NewWriter(database, validationPanicAppender{}, WriterOptions{})
	if err != nil {
		t.Fatalf("NewWriter(second validation writer): %v", err)
	}
	if first.validationGate != second.validationGate {
		t.Fatal("writers over one control DB did not share validation admission")
	}
	if !first.validationGate.TryAcquire() {
		t.Fatal("failed to reserve shared validation gate")
	}
	defer first.validationGate.Release()
	_, err = second.Validate(t.Context(), scope, validationCreateRequest(
		validationAliasDefinition("validation-shared-gate", false),
		opensplunk.KnowledgeValidationIntent_KNOWLEDGE_VALIDATION_INTENT_INACTIVE_STORAGE,
	))
	if !errors.Is(err, control.ErrCapacityExceeded) {
		t.Fatalf("second writer full-gate error = %v, want ErrCapacityExceeded", err)
	}
}

func TestWriterValidateFreshIdentityAuthorityFailsClosedAndClipsWidths(t *testing.T) {
	t.Run("ledger mismatch", func(t *testing.T) {
		database, writer, scope := newWriterValidationHarness(t, true)
		if result := database.GORMDB().Exec(`UPDATE knowledge_catalog_tenants
			SET identity_count = identity_count + 1 WHERE tenant_id = ?`, testTenant); result.Error != nil {
			t.Fatalf("corrupt identity ledger: %v", result.Error)
		}
		_, err := writer.Validate(t.Context(), scope, validationCreateRequest(
			writerActiveRouteDefinition(dependencyExtractionDefinition(
				testApp, "validation-ledger-mismatch", SharingScopePrivate, nil,
				"validation-ledger-*", "validation_ledger_output",
			), "main"),
			opensplunk.KnowledgeValidationIntent_KNOWLEDGE_VALIDATION_INTENT_ACTIVE_PUBLICATION,
		))
		if !errors.Is(err, ErrCorrupt) {
			t.Fatalf("identity-ledger mismatch error = %v, want ErrCorrupt", err)
		}
	})

	t.Run("oversized persisted ID is clipped", func(t *testing.T) {
		database, writer, _ := newWriterValidationHarness(t, false)
		insertFixtureObject(t, database, fixtureObject{
			id: "ko-validation-width", owner: testOwner,
			versions: []fixtureVersion{{
				definition: validationAliasDefinition("validation-width", false),
				state:      StateDraft, mutation: "create", timestamp: 10,
			}},
		})
		dropTrigger(t, database, "knowledge_object_update_requires_sealed_list_projection")
		dropTrigger(t, database, "knowledge_object_registry_transition_is_valid")
		sentinel := "oversized-validation-secret-" + strings.Repeat("x", 1<<20)
		connection, err := database.SQLDB().Conn(t.Context())
		if err != nil {
			t.Fatalf("reserve corruption connection: %v", err)
		}
		if _, err := connection.ExecContext(t.Context(), `PRAGMA foreign_keys = OFF`); err != nil {
			t.Fatalf("disable fixture foreign keys: %v", err)
		}
		if _, err := connection.ExecContext(t.Context(), `PRAGMA ignore_check_constraints = ON`); err != nil {
			t.Fatalf("disable fixture checks: %v", err)
		}
		if _, err := connection.ExecContext(t.Context(), `UPDATE knowledge_objects
			SET knowledge_object_id = ?
			WHERE tenant_id = ? AND knowledge_object_id = ?`,
			sentinel, testTenant, "ko-validation-width",
		); err != nil {
			t.Fatalf("install oversized fixture identity: %v", err)
		}
		if err := connection.Close(); err != nil {
			t.Fatalf("release corruption connection: %v", err)
		}

		tx := writer.orm.WithContext(t.Context()).Begin()
		if tx.Error != nil {
			t.Fatalf("begin fresh-ID width test: %v", tx.Error)
		}
		_, selectErr := selectValidationCandidateID(tx, testTenant)
		_ = tx.Rollback().Error
		if !errors.Is(selectErr, ErrCorrupt) || strings.Contains(selectErr.Error(), sentinel[:64]) {
			t.Fatalf("oversized identity error = %v, want private ErrCorrupt", selectErr)
		}
	})
}

func TestWriterValidateRootAuthorizationAndVersionPrecedeCandidateIssues(t *testing.T) {
	database, writer, scope := newWriterValidationHarness(t, false)
	insertFixtureObject(t, database, fixtureObject{
		id: "ko-validation-root-precedence", owner: testOwner,
		versions: []fixtureVersion{{
			definition: validationAliasDefinition("validation-root-precedence", false),
			state:      StateDraft, mutation: "create", timestamp: 10,
		}},
	})
	invalidDefinition := &opensplunk.KnowledgeObjectDefinition{
		Body: &opensplunk.KnowledgeObjectDefinition_FieldAlias{
			FieldAlias: &opensplunk.FieldAliasDefinition{},
		},
	}
	invalidDefinition.GetFieldAlias().ProtoReflect().SetUnknown(protowire.AppendVarint(
		protowire.AppendTag(nil, 19000, protowire.VarintType),
		1,
	))
	request := validationUpdateRequest(
		"ko-validation-root-precedence",
		1,
		invalidDefinition,
		"field_alias",
	)
	unauthorized := scope
	unauthorized.Write.WritableAppIDs = []string{testAppTwo}
	if _, err := writer.Validate(t.Context(), unauthorized, request); !errors.Is(err, control.ErrNotFound) {
		t.Fatalf("unauthorized root error = %v, want ErrNotFound", err)
	} else if _, ok := AuthorizedContextFromError(err); ok {
		t.Fatalf("unauthorized root error exposed an authorized context: %v", err)
	}
	wrongVersion := proto.Clone(request).(*opensplunk.ValidateKnowledgeObjectRequest)
	wrongVersion.ExpectedVersion = new(uint64(2))
	if _, err := writer.Validate(t.Context(), scope, wrongVersion); !errors.Is(err, control.ErrVersionConflict) {
		t.Fatalf("wrong-version root error = %v, want ErrVersionConflict", err)
	}
	sealed, err := writer.Validate(t.Context(), scope, request)
	if err != nil {
		t.Fatalf("exact authorized invalid candidate: %v", err)
	}
	response, err := sealed.Proto(t.Context())
	if err != nil || response.GetResult().GetValid() ||
		len(response.GetResult().GetFieldViolations()) != 1 {
		t.Fatalf("exact invalid candidate response = %v, %v", response, err)
	}
}

func TestWriterValidateTerminalLifecycleOrdering(t *testing.T) {
	assertAuthorizedRoot := func(t *testing.T, err error, objectID string, version uint64) {
		t.Helper()
		authorized, ok := AuthorizedContextFromError(err)
		if !ok || authorized.AppID != testApp || authorized.Object == nil ||
			authorized.Object.KnowledgeObjectID != objectID ||
			authorized.Object.Version != version {
			t.Fatalf("terminal authorized context = %#v, %t", authorized, ok)
		}
		authorized.Object.KnowledgeObjectID = "caller-mutated"
		again, ok := AuthorizedContextFromError(err)
		if !ok || again.Object == nil || again.Object.KnowledgeObjectID != objectID {
			t.Fatalf("terminal authorized context was not detached: %#v, %t", again, ok)
		}
	}

	t.Run("quarantine wins before retained body hydration", func(t *testing.T) {
		database, writer, scope := newWriterValidationHarness(t, false)
		reason := "root_corruption"
		insertFixtureObject(t, database, fixtureObject{
			id: "ko-validation-quarantined", owner: testOwner,
			versions: []fixtureVersion{
				{
					definition: validationAliasDefinition("retained-quarantine-secret", false),
					state:      StateActive, mutation: "create", timestamp: 10,
				},
				{state: StateQuarantined, mutation: "quarantine", reason: &reason, timestamp: 20},
			},
		})
		dropTrigger(t, database, "knowledge_definition_blob_update_is_forbidden")
		mustExec(t, database, `UPDATE knowledge_definition_blobs
			SET definition_proto = zeroblob(definition_bytes)
			WHERE tenant_id = ?`, testTenant)
		before := readValidationPersistenceSnapshot(t, database)
		candidateSecret := "candidate-quarantine-secret"
		for _, intent := range []opensplunk.KnowledgeValidationIntent{
			opensplunk.KnowledgeValidationIntent_KNOWLEDGE_VALIDATION_INTENT_INACTIVE_STORAGE,
			opensplunk.KnowledgeValidationIntent_KNOWLEDGE_VALIDATION_INTENT_ACTIVE_PUBLICATION,
		} {
			request := validationUpdateRequest(
				"ko-validation-quarantined",
				2,
				&opensplunk.KnowledgeObjectDefinition{Description: &candidateSecret},
				"description",
			)
			request.Intent = intent
			_, err := writer.Validate(t.Context(), scope, request)
			if !errors.Is(err, control.ErrVersionConflict) {
				t.Fatalf("quarantined intent %v error = %v, want ErrVersionConflict", intent, err)
			}
			if strings.Contains(err.Error(), candidateSecret) ||
				strings.Contains(err.Error(), "retained-quarantine-secret") {
				t.Fatalf("quarantined error exposed definition text: %v", err)
			}
			assertAuthorizedRoot(t, err, "ko-validation-quarantined", 2)
		}
		if after := readValidationPersistenceSnapshot(t, database); !reflect.DeepEqual(after, before) {
			t.Fatalf("quarantine validation changed persistence:\nbefore=%#v\nafter=%#v", before, after)
		}
	})

	t.Run("deleted inactive hypothetical but active rejected", func(t *testing.T) {
		database, writer, scope := newWriterValidationHarness(t, false)
		definition := validationAliasDefinition("validation-deleted-current", false)
		insertFixtureObject(t, database, fixtureObject{
			id: "ko-validation-deleted", owner: testOwner,
			versions: []fixtureVersion{
				{definition: definition, state: StateDraft, mutation: "create", timestamp: 10},
				{
					definition: proto.Clone(definition).(*opensplunk.KnowledgeObjectDefinition),
					state:      StateDeleted, mutation: "delete", timestamp: 20,
				},
			},
		})
		before := readValidationPersistenceSnapshot(t, database)
		updatedDescription := "validation-deleted-hypothetical"
		request := validationUpdateRequest(
			"ko-validation-deleted",
			2,
			&opensplunk.KnowledgeObjectDefinition{Description: &updatedDescription},
			"description",
		)
		sealed, err := writer.Validate(t.Context(), scope, request)
		if err != nil {
			t.Fatalf("deleted INACTIVE hypothetical validation: %v", err)
		}
		response, err := sealed.Proto(t.Context())
		if err != nil || !response.GetResult().GetValid() ||
			response.GetResult().GetNormalizedDefinition().GetDescription() != updatedDescription {
			t.Fatalf("deleted INACTIVE response = %v, %v", response, err)
		}

		active := proto.Clone(request).(*opensplunk.ValidateKnowledgeObjectRequest)
		active.Intent = opensplunk.KnowledgeValidationIntent_KNOWLEDGE_VALIDATION_INTENT_ACTIVE_PUBLICATION
		_, activeErr := writer.Validate(t.Context(), scope, active)
		if !errors.Is(activeErr, control.ErrVersionConflict) {
			t.Fatalf("deleted ACTIVE error = %v, want ErrVersionConflict", activeErr)
		}
		if strings.Contains(activeErr.Error(), updatedDescription) {
			t.Fatalf("deleted ACTIVE error exposed candidate definition: %v", activeErr)
		}
		assertAuthorizedRoot(t, activeErr, "ko-validation-deleted", 2)
		if after := readValidationPersistenceSnapshot(t, database); !reflect.DeepEqual(after, before) {
			t.Fatalf("deleted validation changed persistence:\nbefore=%#v\nafter=%#v", before, after)
		}
	})
}

func TestWriterValidateRevisionZeroRequiresEmptyPhysicalRegistry(t *testing.T) {
	t.Run("authorized draft update", func(t *testing.T) {
		database, writer, scope := newWriterValidationHarness(t, true)
		emptyHead := readIntegrationRevisionHead(t, database)
		const objectID = "ko-validation-revision-zero-authorized"
		definition := writerActiveRouteDefinition(
			validationAliasDefinition("validation-revision-zero-authorized", false),
			"main",
		)
		insertFixtureObject(t, database, fixtureObject{
			id: objectID, owner: testOwner,
			versions: []fixtureVersion{{
				definition: definition,
				state:      StateDraft, mutation: "create", timestamp: 10,
			}},
		})
		corruptIntegrationRevisionZeroState(t, database, emptyHead, "")
		before := readValidationPersistenceSnapshot(t, database)
		for _, intent := range []opensplunk.KnowledgeValidationIntent{
			opensplunk.KnowledgeValidationIntent_KNOWLEDGE_VALIDATION_INTENT_INACTIVE_STORAGE,
			opensplunk.KnowledgeValidationIntent_KNOWLEDGE_VALIDATION_INTENT_ACTIVE_PUBLICATION,
		} {
			request := validationUpdateRequest(
				objectID,
				1,
				&opensplunk.KnowledgeObjectDefinition{Name: "validation-revision-zero-authorized"},
				"name",
			)
			request.Intent = intent
			_, err := writer.Validate(t.Context(), scope, request)
			if !errors.Is(err, ErrCorrupt) {
				t.Fatalf("revision-zero update intent %v error = %v, want ErrCorrupt", intent, err)
			}
			if strings.Contains(err.Error(), objectID) {
				t.Fatalf("revision-zero update error disclosed identity/body: %v", err)
			}
			authorized, ok := AuthorizedContextFromError(err)
			if !ok || authorized.AppID != testApp || authorized.Object == nil ||
				authorized.Object.KnowledgeObjectID != objectID || authorized.Object.Version != 1 {
				t.Fatalf("revision-zero update context = %#v, %t", authorized, ok)
			}
		}
		if after := readValidationPersistenceSnapshot(t, database); !reflect.DeepEqual(after, before) {
			t.Fatalf("revision-zero update changed persistence:\nbefore=%#v\nafter=%#v", before, after)
		}
	})

	t.Run("hidden row blocks create generically", func(t *testing.T) {
		database, writer, scope := newWriterValidationHarness(t, false)
		emptyHead := readIntegrationRevisionHead(t, database)
		const hiddenID = "ko-validation-revision-zero-hidden"
		insertFixtureObject(t, database, fixtureObject{
			id: hiddenID, owner: "owner-hidden",
			versions: []fixtureVersion{{
				definition: validationAliasDefinition("revision-zero-hidden-body", false),
				state:      StateDraft, mutation: "create", timestamp: 10,
			}},
		})
		corruptIntegrationRevisionZeroState(t, database, emptyHead, "")
		before := readValidationPersistenceSnapshot(t, database)
		_, err := writer.Validate(t.Context(), scope, validationCreateRequest(
			validationAliasDefinition("validation-revision-zero-create", false),
			opensplunk.KnowledgeValidationIntent_KNOWLEDGE_VALIDATION_INTENT_INACTIVE_STORAGE,
		))
		if !errors.Is(err, ErrCorrupt) {
			t.Fatalf("revision-zero hidden-row create error = %v, want ErrCorrupt", err)
		}
		if strings.Contains(err.Error(), hiddenID) || strings.Contains(err.Error(), "hidden-body") {
			t.Fatalf("revision-zero hidden-row error disclosed hidden authority: %v", err)
		}
		authorized, ok := AuthorizedContextFromError(err)
		if !ok || authorized.AppID != testApp || authorized.Object != nil {
			t.Fatalf("revision-zero create context = %#v, %t", authorized, ok)
		}
		if after := readValidationPersistenceSnapshot(t, database); !reflect.DeepEqual(after, before) {
			t.Fatalf("revision-zero create changed persistence:\nbefore=%#v\nafter=%#v", before, after)
		}
	})
}

func TestWriterValidateProductionPathAlwaysRollsBackInjectedDML(t *testing.T) {
	database, writer, scope := newWriterValidationHarness(t, false)
	if err := database.GORMDB().Exec(`CREATE TABLE validation_rollback_sentinel (
		value INTEGER NOT NULL
	)`).Error; err != nil {
		t.Fatalf("create validation rollback sentinel: %v", err)
	}
	callback := writer.orm.Callback().Query()
	const callbackName = "validation_test:inject_rollback_sentinel"
	var injected atomic.Bool
	if err := callback.Before("gorm:query").Register(callbackName, func(tx *gorm.DB) {
		if !injected.CompareAndSwap(false, true) {
			return
		}
		if err := tx.Exec(`INSERT INTO validation_rollback_sentinel (value) VALUES (1)`).Error; err != nil {
			_ = tx.AddError(err)
		}
	}); err != nil {
		t.Fatalf("register validation rollback sentinel: %v", err)
	}
	removed := false
	t.Cleanup(func() {
		if !removed {
			_ = callback.Remove(callbackName)
		}
	})
	sealed, err := writer.Validate(t.Context(), scope, validationCreateRequest(
		validationAliasDefinition("validation-rollback-sentinel", false),
		opensplunk.KnowledgeValidationIntent_KNOWLEDGE_VALIDATION_INTENT_INACTIVE_STORAGE,
	))
	if err != nil {
		t.Fatalf("Validate(injected rollback DML): %v", err)
	}
	if response, protoErr := sealed.Proto(t.Context()); protoErr != nil || !response.GetResult().GetValid() {
		t.Fatalf("injected rollback response = %v, %v", response, protoErr)
	}
	if !injected.Load() {
		t.Fatal("validation query callback did not inject sentinel DML")
	}
	if err := callback.Remove(callbackName); err != nil {
		t.Fatalf("remove validation rollback sentinel: %v", err)
	}
	removed = true
	var count int64
	if err := database.GORMDB().Table("validation_rollback_sentinel").Count(&count).Error; err != nil {
		t.Fatalf("count validation rollback sentinel: %v", err)
	}
	if count != 0 {
		t.Fatalf("validation committed %d injected sentinel rows, want rollback", count)
	}
}

func TestValidationBoundaryErrorTaxonomy(t *testing.T) {
	tooLarge := validationSealError(knowledgevalidation.ErrResponseTooLarge)
	if !errors.Is(tooLarge, knowledgevalidation.ErrResponseTooLarge) ||
		!errors.Is(tooLarge, control.ErrCapacityExceeded) {
		t.Fatalf("response-too-large taxonomy = %v", tooLarge)
	}
	targetDrift := validationTargetIntegrityError(fmt.Errorf(
		"private target detail: %w",
		control.ErrDependencyConflict,
	))
	if !errors.Is(targetDrift, ErrCorrupt) || errors.Is(targetDrift, control.ErrDependencyConflict) ||
		strings.Contains(targetDrift.Error(), "private target detail") {
		t.Fatalf("target-integrity taxonomy = %v", targetDrift)
	}
}

func TestFinishValidationTransactionRejectsPreEndedTransaction(t *testing.T) {
	database, _, _ := newWriterValidationHarness(t, false)
	tx := database.GORMDB().WithContext(t.Context()).Begin()
	if tx.Error != nil {
		t.Fatalf("begin pre-ended validation transaction: %v", tx.Error)
	}
	if err := tx.Commit().Error; err != nil {
		t.Fatalf("pre-end validation transaction: %v", err)
	}
	var returnedErr error
	finishValidationTransaction(tx, &returnedErr)
	if returnedErr == nil || !strings.Contains(returnedErr.Error(), "roll back knowledge validation") {
		t.Fatalf("pre-ended validation cleanup error = %v", returnedErr)
	}
}

func archiveValidationApp(t *testing.T, database *control.DB, appID string) {
	t.Helper()
	appCatalog, err := control.NewAppCatalog(database, control.AppCatalogOptions{
		CursorKey: testCursorKey,
		Clock:     func() time.Time { return time.UnixMicro(100_000).UTC() },
	})
	if err != nil {
		t.Fatalf("NewAppCatalog(archive validation app): %v", err)
	}
	workspace, err := appCatalog.GetApp(
		t.Context(),
		control.AppAccessScope{TenantID: testTenant},
		control.AppSelector{AppID: appID},
	)
	if err != nil {
		t.Fatalf("GetApp(%s): %v", appID, err)
	}
	if _, err := appCatalog.SetAppState(
		t.Context(),
		control.AppAccessScope{TenantID: testTenant},
		control.AppSelector{AppID: appID},
		workspace.Version,
		control.AppStateArchived,
	); err != nil {
		t.Fatalf("archive app %s: %v", appID, err)
	}
}
