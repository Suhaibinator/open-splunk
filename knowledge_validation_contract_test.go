package opensplunk_test

import (
	"bytes"
	"cmp"
	"fmt"
	"os"
	"slices"
	"strings"
	"testing"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

func TestKnowledgeValidationContractPinsCandidateScopedWireLayout(t *testing.T) {
	t.Parallel()

	file := opensplunkv1.File_open_splunk_v1_knowledge_api_proto
	intent := file.Enums().ByName("KnowledgeValidationIntent")
	if intent == nil || intent.Values().Len() != 3 {
		t.Fatalf("KnowledgeValidationIntent descriptor = %v, want three values", intent)
	}
	for name, number := range map[protoreflect.Name]protoreflect.EnumNumber{
		"KNOWLEDGE_VALIDATION_INTENT_UNSPECIFIED":        0,
		"KNOWLEDGE_VALIDATION_INTENT_INACTIVE_STORAGE":   1,
		"KNOWLEDGE_VALIDATION_INTENT_ACTIVE_PUBLICATION": 2,
	} {
		value := intent.Values().ByName(name)
		if value == nil || value.Number() != number {
			t.Errorf("KnowledgeValidationIntent.%s = %v, want %d", name, value, number)
		}
	}
	validationContractRequireProtoComments(
		t,
		"enum KnowledgeValidationIntent {",
		"exactly one of the two defined nonzero",
		"unknown numeric enum value",
	)
	validationContractRequireProtoComments(
		t,
		"KNOWLEDGE_VALIDATION_INTENT_INACTIVE_STORAGE = 1;",
		"returns no derived dependencies",
	)
	validationContractRequireProtoComments(
		t,
		"KNOWLEDGE_VALIDATION_INTENT_ACTIVE_PUBLICATION = 2;",
		"one fixed knowledge, app, and index catalog transaction",
		"only the knowledge-ledger component",
		"not the complete transaction authority",
	)

	request := file.Messages().ByName("ValidateKnowledgeObjectRequest")
	if request == nil || request.Fields().Len() != 5 {
		t.Fatalf("ValidateKnowledgeObjectRequest descriptor = %v, want five fields", request)
	}
	definition := validationContractRequireField(t, request, "definition", 1, protoreflect.MessageKind, "open_splunk.v1.KnowledgeObjectDefinition")
	objectID := validationContractRequireField(t, request, "knowledge_object_id", 2, protoreflect.StringKind, "")
	expectedVersion := validationContractRequireField(t, request, "expected_version", 3, protoreflect.Uint64Kind, "")
	updateMask := validationContractRequireField(t, request, "update_mask", 4, protoreflect.MessageKind, "google.protobuf.FieldMask")
	intentField := validationContractRequireField(t, request, "intent", 5, protoreflect.EnumKind, "open_splunk.v1.KnowledgeValidationIntent")
	if !objectID.HasPresence() || !objectID.HasOptionalKeyword() ||
		!expectedVersion.HasPresence() || !expectedVersion.HasOptionalKeyword() {
		t.Error("create/update scalar presence is not pinned by proto3 optional fields")
	}
	if !updateMask.HasPresence() || updateMask.HasOptionalKeyword() {
		t.Error("update_mask must retain ordinary message presence")
	}
	if !definition.HasPresence() || definition.HasOptionalKeyword() {
		t.Error("definition must retain required-by-boundary ordinary message presence")
	}
	if intentField.HasPresence() || intentField.HasOptionalKeyword() {
		t.Error("intent must remain a required-by-validation scalar enum")
	}
	validationContractRequireProtoComments(t, "message ValidateKnowledgeObjectRequest {",
		"definition message presence is required",
		"present definition whose body is missing or unknown",
		"exactly INACTIVE_STORAGE or ACTIVE_PUBLICATION",
		"unknown numeric values",
		"message to both be absent, not merely zero or empty",
		"deterministic, non-persisted object ID proven fresh in the same catalog transaction",
		"invariant under alpha-renaming that ID to any other fresh valid ID",
		"neither reserves that ID nor authorizes a later Create",
		"Create generates its own ID and revalidates the then-current catalog, app, and index authority",
		"intervening changes may alter the outcome",
		"inclusive range 1 through MaxInt64 (9223372036854775807)",
		"present message with at least one canonical path",
		"valid=false candidate result",
	)

	previewRequest := file.Messages().ByName("PreviewKnowledgeObjectRequest")
	if previewRequest == nil || previewRequest.Fields().Len() != 6 {
		t.Fatalf("PreviewKnowledgeObjectRequest descriptor = %v, want six fields", previewRequest)
	}
	retainedJobID := validationContractRequireField(t, previewRequest, "retained_search_job_id", 1, protoreflect.StringKind, "")
	previewDefinition := validationContractRequireField(t, previewRequest, "definition", 2, protoreflect.MessageKind, "open_splunk.v1.KnowledgeObjectDefinition")
	previewObjectID := validationContractRequireField(t, previewRequest, "knowledge_object_id", 3, protoreflect.StringKind, "")
	previewExpectedVersion := validationContractRequireField(t, previewRequest, "expected_version", 4, protoreflect.Uint64Kind, "")
	previewUpdateMask := validationContractRequireField(t, previewRequest, "update_mask", 5, protoreflect.MessageKind, "google.protobuf.FieldMask")
	previewMaximumRows := validationContractRequireField(t, previewRequest, "maximum_rows", 6, protoreflect.Uint32Kind, "")
	if retainedJobID.HasPresence() || retainedJobID.HasOptionalKeyword() {
		t.Error("retained_search_job_id must remain a required-by-validation proto3 scalar")
	}
	if !previewDefinition.HasPresence() || previewDefinition.HasOptionalKeyword() ||
		!previewUpdateMask.HasPresence() || previewUpdateMask.HasOptionalKeyword() {
		t.Error("Preview definition and update_mask must retain ordinary message presence")
	}
	if !previewObjectID.HasPresence() || !previewObjectID.HasOptionalKeyword() ||
		!previewExpectedVersion.HasPresence() || !previewExpectedVersion.HasOptionalKeyword() ||
		!previewMaximumRows.HasPresence() || !previewMaximumRows.HasOptionalKeyword() {
		t.Error("Preview create/update and maximum_rows scalar presence is not pinned by proto3 optional fields")
	}
	validationContractRequireProtoComments(t, "message PreviewKnowledgeObjectRequest {",
		"future route remains unregistered",
		"internal request codec accepts a raw protobuf request body of at most 4 MiB plus 64 KiB (4259840 bytes)",
		"future owner-scoped retained execution authority",
		"reacquire under the authenticated caller",
		"not an immutable event snapshot identity",
		"does not itself grant access",
		"nonempty valid UTF-8 of at most 256 bytes",
		"unchanged by whitespace trimming",
		"no Unicode control code point",
		"rejects malformed wire and unknown-group nesting deeper than 32",
		"retains at most 9 update-mask paths and 17 entries in each selected selector dimension or regex output list",
		"validating UTF-8 in every recognized string occurrence",
		"including values later overwritten, unselected, or cleared",
		"outer and wrong-wire envelope unknowns plus update-mask unknowns for structural rejection",
		"Create full-candidate unknowns and update mask-selected nested unknowns are retained",
		"update candidate top-level and unselected nested unknowns are discarded",
		"exact create/update envelope of ValidateKnowledgeObjectRequest",
		"server forcing ACTIVE_PUBLICATION",
		"structural envelope validator performs no retained-job lookup or authorization",
		"never mutates or normalizes the decoded request",
	)
	validationContractRequireNestedProtoComments(t, "message PreviewKnowledgeObjectRequest {", "optional uint32 maximum_rows = 6;",
		"preserves only presence and the exact uint32 value",
		"no default, bound, or execution meaning",
		"future Preview service contract",
	)

	dependency := file.Messages().ByName("KnowledgeValidationDependency")
	if dependency == nil || dependency.Fields().Len() != 2 {
		t.Fatalf("KnowledgeValidationDependency descriptor = %v, want two fields", dependency)
	}
	validationContractRequireField(t, dependency, "target", 1, protoreflect.MessageKind, "open_splunk.v1.KnowledgeManagementObjectVersionIdentity")
	validationContractRequireField(t, dependency, "role", 2, protoreflect.EnumKind, "open_splunk.v1.KnowledgeDependencyRole")
	validationContractRequireProtoComments(t, "message KnowledgeValidationDependency {",
		"excludes a source identity, definition digests",
		"Only ACTIVE_PUBLICATION may",
		"only FIELD_INPUT",
		"Missing and unauthorized dependency targets are indistinguishable",
		"KNOWLEDGE_DEPENDENCY_UNAVAILABLE",
		"uniform non-2xx response",
	)

	diagnostic := file.Messages().ByName("KnowledgeValidationDiagnostic")
	if diagnostic == nil || diagnostic.Fields().Len() != 2 {
		t.Fatalf("KnowledgeValidationDiagnostic descriptor = %v, want two fields", diagnostic)
	}
	validationContractRequireField(t, diagnostic, "field_path", 1, protoreflect.StringKind, "")
	validationContractRequireField(t, diagnostic, "diagnostic", 2, protoreflect.MessageKind, "open_splunk.v1.Diagnostic")
	validationContractRequireProtoComments(t, "message KnowledgeValidationDiagnostic {",
		"offsets are relative to the UTF-8 scalar value at field_path",
		"present source_range requires",
		"nonnil start and end positions",
		"half-open range with start byte",
		"at most the exact scalar",
		"fall on code-point boundaries",
		"uniquely derived one-based coordinates",
		"LF increments line and resets column to 1",
		"including CR, increments column",
		"UNSPECIFIED and unknown diagnostic severities",
		"at most 32 unique values",
		"stable static templates plus exact source text",
		"any other catalog object, app, owner, name, ID, version, digest, definition",
		"index inventory beyond candidate-authored text",
		"generated SQL, or hidden authority",
	)

	resources := file.Messages().ByName("KnowledgeResourceEstimate")
	if resources == nil || resources.Fields().Len() != 13 {
		t.Fatalf("KnowledgeResourceEstimate descriptor = %v, want thirteen fields", resources)
	}
	resourceFields := []struct {
		name   protoreflect.Name
		number protoreflect.FieldNumber
		kind   protoreflect.Kind
	}{
		{name: "selector_patterns", number: 1, kind: protoreflect.Uint32Kind},
		{name: "normalized_definition_bytes", number: 2, kind: protoreflect.Uint64Kind},
		{name: "dependency_nodes", number: 3, kind: protoreflect.Uint32Kind},
		{name: "dependency_edges", number: 4, kind: protoreflect.Uint32Kind},
		{name: "generated_operators", number: 5, kind: protoreflect.Uint32Kind},
		{name: "generated_fields", number: 6, kind: protoreflect.Uint32Kind},
		{name: "regex_programs", number: 7, kind: protoreflect.Uint32Kind},
		{name: "estimated_regex_work_units", number: 8, kind: protoreflect.Uint64Kind},
		{name: "scalar_expressions", number: 9, kind: protoreflect.Uint32Kind},
		{name: "scalar_expression_nodes", number: 10, kind: protoreflect.Uint32Kind},
		{name: "extraction_outputs", number: 12, kind: protoreflect.Uint32Kind},
		{name: "json_evaluation_work_units", number: 13, kind: protoreflect.Uint32Kind},
		{name: "scalar_predicates", number: 14, kind: protoreflect.Uint32Kind},
	}
	for _, expected := range resourceFields {
		field := validationContractRequireField(t, resources, expected.name, expected.number, expected.kind, "")
		if field.HasPresence() || field.HasOptionalKeyword() {
			t.Errorf("KnowledgeResourceEstimate.%s must remain a zero-default proto3 scalar", expected.name)
		}
	}
	if resources.Fields().ByNumber(11) != nil || !validationContractNumberReserved(resources, 11) ||
		!validationContractNameReserved(resources, "estimated_generated_sql_bytes") {
		t.Error("undefined generated-SQL estimate field 11/name are not both reserved")
	}
	validationContractRequireProtoComments(t, "message KnowledgeResourceEstimate {",
		"only to the applied candidate",
		"partial resource estimates are forbidden",
		"exact number of normalized selector patterns",
		"exact deterministic protobuf size",
		"INACTIVE_STORAGE",
		"dependency_nodes, dependency_edges, and every compile-",
		"extraction_outputs, json_evaluation_work_units, and scalar_predicates",
		"exactly zero because publication compilation does not occur",
		"ACTIVE_PUBLICATION",
		"only object is the applied normalized",
		"dependency list is empty",
		"Charges.GeneratedOperators",
		"Charges.GeneratedFields",
		"Charges.RegexPrograms",
		"Charges.RegexWorkUnits",
		"Charges.ScalarExpressions",
		"Charges.ScalarExpressionNodes",
		"Charges.ExtractionOutputs",
		"Charges.JSONEvaluationWork",
		"Charges.ScalarPredicates",
		"neither affected-cohort totals nor",
		"marginal deltas after cohort operator fusion",
		"remain derived from the full ACTIVE transition",
		"not from the singleton program",
	)
	validationContractRequireProtoComments(t, "uint32 selector_patterns = 1;",
		"Exact number of normalized selector patterns in the applied candidate",
	)
	validationContractRequireProtoComments(t, "reserved 11;",
		"Intentional historical FILE compatibility waiver",
		"field was retired before Validate was registered",
		"never served by either the validate or preview route",
		"may drop this never-served field",
		"must not be described as schema non-breaking",
	)

	result := file.Messages().ByName("KnowledgeValidationResult")
	if result == nil || result.Fields().Len() != 10 {
		t.Fatalf("KnowledgeValidationResult descriptor = %v, want ten fields", result)
	}
	for _, number := range []protoreflect.FieldNumber{6, 7} {
		if result.Fields().ByNumber(number) != nil || !validationContractNumberReserved(result, number) {
			t.Errorf("KnowledgeValidationResult pre-route draft field %d is not reserved", number)
		}
	}
	validationContractRequireField(t, result, "valid", 1, protoreflect.BoolKind, "")
	validationContractRequireField(t, result, "object_type", 2, protoreflect.EnumKind, "open_splunk.v1.KnowledgeObjectType")
	normalized := validationContractRequireField(t, result, "normalized_definition", 3, protoreflect.MessageKind, "open_splunk.v1.KnowledgeObjectDefinition")
	digest := validationContractRequireField(t, result, "definition_sha256", 4, protoreflect.BytesKind, "")
	fieldViolations := validationContractRequireField(t, result, "field_violations", 5, protoreflect.MessageKind, "open_splunk.v1.FieldViolation")
	resourcesField := validationContractRequireField(t, result, "resources", 8, protoreflect.MessageKind, "open_splunk.v1.KnowledgeResourceEstimate")
	dependencies := validationContractRequireField(t, result, "dependencies", 9, protoreflect.MessageKind, "open_splunk.v1.KnowledgeValidationDependency")
	diagnostics := validationContractRequireField(t, result, "diagnostics", 10, protoreflect.MessageKind, "open_splunk.v1.KnowledgeValidationDiagnostic")
	violationsTruncated := validationContractRequireField(t, result, "field_violations_truncated", 11, protoreflect.BoolKind, "")
	diagnosticsTruncated := validationContractRequireField(t, result, "diagnostics_truncated", 12, protoreflect.BoolKind, "")
	if !normalized.HasPresence() || !normalized.HasOptionalKeyword() ||
		!digest.HasPresence() || !digest.HasOptionalKeyword() {
		t.Error("successful-result definition and digest must retain explicit presence")
	}
	if dependencies.Cardinality() != protoreflect.Repeated || diagnostics.Cardinality() != protoreflect.Repeated {
		t.Error("candidate dependencies and diagnostics must remain repeated projections")
	}
	if fieldViolations.Cardinality() != protoreflect.Repeated || fieldViolations.HasPresence() ||
		resourcesField.Cardinality() != protoreflect.Optional || !resourcesField.HasPresence() {
		t.Error("field_violations must remain repeated and resources must remain one presence-bearing message")
	}
	if violationsTruncated.HasPresence() || diagnosticsTruncated.HasPresence() {
		t.Error("truncation flags must retain ordinary false-when-complete proto3 bool semantics")
	}
	validationContractRequireProtoComments(t, "message KnowledgeValidationResult {",
		"definition validity",
		"valid is not mutation",
		"acceptability, a reservation, or a promise",
		"masked update that is identical to the current",
		"may be valid",
		"INACTIVE_STORAGE against a currently ACTIVE object",
		"hypothetical non-ACTIVE storage validity",
		"never ACTIVE Update",
		"Every later Writer operation independently revalidates",
		"then-current authorization, version, lifecycle, capacity, app, index, and",
		"valid=false requires at least one retained field violation or ERROR",
		"valid=true requires a normalized_definition, an exact 32-byte SHA-256 digest",
		"dependencies, and resources to be",
		"Persisted corruption and hidden inventory failures",
		"uniform non-2xx",
		"recursively rejects unknown protobuf fields in this result and every nested",
		"keys cover every currently recognized nested field",
		"future appended issue field must extend per-entry validation, deduplication, and comparison",
	)
	validationContractRequireProtoComments(t, "repeated FieldViolation field_violations = 5;",
		"256 KiB (262144-byte)",
		"deduplicate exact full values",
		"byte lengths of field_path, code, and message",
		"field_violations count or aggregate-text bound",
		"static-template, candidate-source-only nondisclosure rule",
	)
	validationContractRequireProtoComments(t, "reserved 6, 7;",
		"Intentional historical FILE compatibility waiver",
		"fields were retired before Validate",
		"never served by either the validate or preview route",
		"may drop these never-served fields",
		"must not be described as schema non-breaking",
	)
	validationContractRequireProtoComments(t, "repeated KnowledgeValidationDependency dependencies = 9;",
		"at most 1024 unique values",
		"INACTIVE_STORAGE always returns an empty list",
	)
	validationContractRequireProtoComments(t, "repeated KnowledgeValidationDiagnostic diagnostics = 10;",
		"768 KiB (786432-byte)",
		"absent source range before a",
		"ERROR, WARNING, INFO",
		"UNSPECIFIED and unknown severities are invalid",
		"suggestions sequence lexicographically",
		"canonical derived start",
		"deduplicate exact full values",
		"every suggestion, without separators or wire framing",
		"diagnostics count or aggregate-text bound",
		"valid=false retains at least one ERROR diagnostic",
	)
	validationContractRequireProtoComments(t, "uint32 dependency_nodes = 3;",
		"Distinct exact direct targets in result.dependencies",
		"zero for INACTIVE_STORAGE",
		"valid ACTIVE_PUBLICATION",
		"returned authorized dependencies",
	)
	validationContractRequireProtoComments(t, "uint32 dependency_edges = 4;",
		"equals result.dependencies size",
		"zero for INACTIVE_STORAGE",
		"returned authorized dependencies",
	)
	validationContractRequireProtoComments(t, "bool field_violations_truncated = 11;",
		"True exactly when",
		"false means",
		"false when valid=true",
	)
	validationContractRequireProtoComments(t, "bool diagnostics_truncated = 12;",
		"True exactly when",
		"false means",
	)

	response := file.Messages().ByName("ValidateKnowledgeObjectResponse")
	if response == nil || response.Fields().Len() != 2 {
		t.Fatalf("ValidateKnowledgeObjectResponse descriptor = %v, want two fields", response)
	}
	validationContractRequireField(t, response, "result", 1, protoreflect.MessageKind, "open_splunk.v1.KnowledgeValidationResult")
	validationContractRequireField(t, response, "tenant_catalog_revision", 2, protoreflect.Uint64Kind, "")
	resultField := response.Fields().ByName("result")
	revisionField := response.Fields().ByName("tenant_catalog_revision")
	if resultField == nil || !resultField.HasPresence() || resultField.HasOptionalKeyword() ||
		revisionField == nil || revisionField.HasPresence() {
		t.Error("validation response must retain boundary-required result presence and scalar revision")
	}
	validationContractRequireProtoComments(t, "message ValidateKnowledgeObjectResponse {",
		"knowledge, app, and index catalogs in one fixed",
		"deterministic protobuf encoding of this complete response",
		"8 MiB (8388608 bytes)",
		"recursively rejects unknown protobuf fields in the response",
		"before deterministic serialization",
	)
	validationContractRequireNestedProtoComments(t, "message ValidateKnowledgeObjectResponse {", "KnowledgeValidationResult result = 1;",
		"Required by the response boundary",
		"absent result is invalid",
	)
	validationContractRequireNestedProtoComments(t, "message ValidateKnowledgeObjectResponse {", "uint64 tenant_catalog_revision = 2;",
		"Exact knowledge-ledger revision observed in the fixed transaction",
		"zero",
		"proven empty knowledge ledger",
		"only the knowledge-",
		"not the app or index authority",
		"advisory correlation metadata",
		"not a reusable full authority",
		"reservation, mutation proof, or promise",
	)

	previewResponse := file.Messages().ByName("PreviewKnowledgeObjectResponse")
	if previewResponse == nil {
		t.Fatal("PreviewKnowledgeObjectResponse descriptor is missing")
	}
	validationContractRequireField(t, previewResponse, "validation", 1, protoreflect.MessageKind, "open_splunk.v1.KnowledgeValidationResult")
	validationContractRequireField(t, previewResponse, "tenant_catalog_revision", 7, protoreflect.Uint64Kind, "")
	validationContractRequireProtoComments(t, "message PreviewKnowledgeObjectResponse {",
		"future unregistered route",
		"no independent validation intent",
		"always uses ACTIVE_PUBLICATION",
		"same create/update candidate-envelope semantics as ValidateKnowledgeObjectRequest",
		"definition validity in one fixed knowledge, app, and index catalog",
		"dependency, resource, truncation, and nondisclosure invariant",
		"tenant_catalog_revision identifies only the advisory",
		"not mutation acceptability, a reservation",
		"reusable publication proof",
		"every later Writer operation revalidates",
	)
}

func TestKnowledgeValidationContractPreservesPresenceAndReservedDraftWire(t *testing.T) {
	t.Parallel()

	emptyID := ""
	zeroVersion := uint64(0)
	update := &opensplunkv1.ValidateKnowledgeObjectRequest{
		KnowledgeObjectId: &emptyID,
		ExpectedVersion:   &zeroVersion,
		UpdateMask:        &fieldmaskpb.FieldMask{},
		Intent:            opensplunkv1.KnowledgeValidationIntent_KNOWLEDGE_VALIDATION_INTENT_ACTIVE_PUBLICATION,
	}
	updateWire, err := proto.MarshalOptions{Deterministic: true}.Marshal(update)
	if err != nil {
		t.Fatalf("marshal present-empty update envelope: %v", err)
	}
	if got := validationContractWireFieldNumbers(t, updateWire); !slices.Equal(got, []protowire.Number{2, 3, 4, 5}) {
		t.Fatalf("present-empty update wire fields = %v, want [2 3 4 5]", got)
	}
	var decodedUpdate opensplunkv1.ValidateKnowledgeObjectRequest
	if err := proto.Unmarshal(updateWire, &decodedUpdate); err != nil {
		t.Fatalf("unmarshal present-empty update envelope: %v", err)
	}
	if decodedUpdate.KnowledgeObjectId == nil || *decodedUpdate.KnowledgeObjectId != "" ||
		decodedUpdate.ExpectedVersion == nil || *decodedUpdate.ExpectedVersion != 0 ||
		decodedUpdate.UpdateMask == nil || len(decodedUpdate.UpdateMask.GetPaths()) != 0 {
		t.Fatalf("present-empty update envelope lost presence: %+v", &decodedUpdate)
	}

	create := &opensplunkv1.ValidateKnowledgeObjectRequest{
		Definition: validationContractMinimalDefinition(),
		Intent:     opensplunkv1.KnowledgeValidationIntent_KNOWLEDGE_VALIDATION_INTENT_INACTIVE_STORAGE,
	}
	createWire, err := proto.MarshalOptions{Deterministic: true}.Marshal(create)
	if err != nil {
		t.Fatalf("marshal create envelope: %v", err)
	}
	if got := validationContractWireFieldNumbers(t, createWire); !slices.Equal(got, []protowire.Number{1, 5}) {
		t.Fatalf("create wire fields = %v, want definition/intent fields [1 5]", got)
	}
	var decodedCreate opensplunkv1.ValidateKnowledgeObjectRequest
	if err := proto.Unmarshal(createWire, &decodedCreate); err != nil {
		t.Fatalf("unmarshal create envelope: %v", err)
	}
	if decodedCreate.GetDefinition() == nil || decodedCreate.GetDefinition().GetFieldAlias() == nil ||
		decodedCreate.KnowledgeObjectId != nil || decodedCreate.ExpectedVersion != nil || decodedCreate.UpdateMask != nil {
		t.Fatalf("create envelope acquired update presence: %+v", &decodedCreate)
	}

	previewCreate := &opensplunkv1.PreviewKnowledgeObjectRequest{
		RetainedSearchJobId: "job-preview-create",
		Definition:          validationContractMinimalDefinition(),
	}
	previewCreateWire, err := proto.MarshalOptions{Deterministic: true}.Marshal(previewCreate)
	if err != nil {
		t.Fatalf("marshal Preview create envelope: %v", err)
	}
	if got := validationContractWireFieldNumbers(t, previewCreateWire); !slices.Equal(got, []protowire.Number{1, 2}) {
		t.Fatalf("Preview create wire fields = %v, want retained job/definition fields [1 2]", got)
	}
	var decodedPreviewCreate opensplunkv1.PreviewKnowledgeObjectRequest
	if err := proto.Unmarshal(previewCreateWire, &decodedPreviewCreate); err != nil {
		t.Fatalf("unmarshal Preview create envelope: %v", err)
	}
	if decodedPreviewCreate.GetRetainedSearchJobId() != "job-preview-create" ||
		decodedPreviewCreate.GetDefinition() == nil || decodedPreviewCreate.KnowledgeObjectId != nil ||
		decodedPreviewCreate.ExpectedVersion != nil || decodedPreviewCreate.UpdateMask != nil ||
		decodedPreviewCreate.MaximumRows != nil {
		t.Fatalf("Preview create envelope presence changed: %+v", &decodedPreviewCreate)
	}

	zeroRows := uint32(0)
	previewPresentEmptyUpdate := &opensplunkv1.PreviewKnowledgeObjectRequest{
		RetainedSearchJobId: "job-preview-update",
		Definition:          validationContractMinimalDefinition(),
		KnowledgeObjectId:   &emptyID,
		ExpectedVersion:     &zeroVersion,
		UpdateMask:          &fieldmaskpb.FieldMask{},
		MaximumRows:         &zeroRows,
	}
	previewPresentEmptyUpdateWire, err := proto.MarshalOptions{Deterministic: true}.Marshal(previewPresentEmptyUpdate)
	if err != nil {
		t.Fatalf("marshal present-empty Preview update envelope: %v", err)
	}
	if got := validationContractWireFieldNumbers(t, previewPresentEmptyUpdateWire); !slices.Equal(
		got,
		[]protowire.Number{1, 2, 3, 4, 5, 6},
	) {
		t.Fatalf("present-empty Preview update wire fields = %v, want [1 2 3 4 5 6]", got)
	}
	var decodedPreviewUpdate opensplunkv1.PreviewKnowledgeObjectRequest
	if err := proto.Unmarshal(previewPresentEmptyUpdateWire, &decodedPreviewUpdate); err != nil {
		t.Fatalf("unmarshal present-empty Preview update envelope: %v", err)
	}
	if decodedPreviewUpdate.KnowledgeObjectId == nil || *decodedPreviewUpdate.KnowledgeObjectId != "" ||
		decodedPreviewUpdate.ExpectedVersion == nil || *decodedPreviewUpdate.ExpectedVersion != 0 ||
		decodedPreviewUpdate.UpdateMask == nil || len(decodedPreviewUpdate.UpdateMask.GetPaths()) != 0 ||
		decodedPreviewUpdate.MaximumRows == nil || *decodedPreviewUpdate.MaximumRows != 0 {
		t.Fatalf("present-empty Preview update envelope lost presence: %+v", &decodedPreviewUpdate)
	}

	resourceEstimate := &opensplunkv1.KnowledgeResourceEstimate{
		SelectorPatterns:          1,
		NormalizedDefinitionBytes: 2,
		DependencyNodes:           3,
		DependencyEdges:           4,
		GeneratedOperators:        5,
		GeneratedFields:           6,
		RegexPrograms:             7,
		EstimatedRegexWorkUnits:   8,
		ScalarExpressions:         9,
		ScalarExpressionNodes:     10,
		ExtractionOutputs:         12,
		JsonEvaluationWorkUnits:   13,
		ScalarPredicates:          14,
	}
	resourceWire, err := proto.MarshalOptions{Deterministic: true}.Marshal(resourceEstimate)
	if err != nil {
		t.Fatalf("marshal complete candidate resource estimate: %v", err)
	}
	if got := validationContractWireFieldNumbers(t, resourceWire); !slices.Equal(
		got,
		[]protowire.Number{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 13, 14},
	) {
		t.Fatalf("candidate resource wire fields = %v, want append-only [1..10 12 13 14]", got)
	}
	var decodedResources opensplunkv1.KnowledgeResourceEstimate
	if err := proto.Unmarshal(resourceWire, &decodedResources); err != nil {
		t.Fatalf("unmarshal complete candidate resource estimate: %v", err)
	}
	if !proto.Equal(&decodedResources, resourceEstimate) {
		t.Fatalf("candidate resource wire round trip = %+v, want %+v", &decodedResources, resourceEstimate)
	}
	emptyResourceWire, err := proto.MarshalOptions{Deterministic: true}.Marshal(
		&opensplunkv1.KnowledgeResourceEstimate{},
	)
	if err != nil {
		t.Fatalf("marshal empty candidate resource estimate: %v", err)
	}
	if len(emptyResourceWire) != 0 {
		t.Fatalf("zero-default candidate resource wire = %x, want empty", emptyResourceWire)
	}

	legacyDiagnostic, err := proto.Marshal(&opensplunkv1.Diagnostic{Code: "LEGACY_UNLOCATED"})
	if err != nil {
		t.Fatalf("marshal legacy diagnostic: %v", err)
	}
	legacyDependency, err := proto.Marshal(&opensplunkv1.KnowledgeObjectDependency{})
	if err != nil {
		t.Fatalf("marshal legacy dependency: %v", err)
	}
	legacyTopLevelUnknown := protowire.AppendTag(nil, 6, protowire.BytesType)
	legacyTopLevelUnknown = protowire.AppendBytes(legacyTopLevelUnknown, legacyDiagnostic)
	legacyTopLevelUnknown = protowire.AppendTag(legacyTopLevelUnknown, 7, protowire.BytesType)
	legacyTopLevelUnknown = protowire.AppendBytes(legacyTopLevelUnknown, legacyDependency)
	legacyResourceUnknown := protowire.AppendTag(nil, 11, protowire.VarintType)
	legacyResourceUnknown = protowire.AppendVarint(legacyResourceUnknown, 99)
	legacyWire := append([]byte(nil), legacyTopLevelUnknown...)
	legacyWire = protowire.AppendTag(legacyWire, 8, protowire.BytesType)
	legacyWire = protowire.AppendBytes(legacyWire, legacyResourceUnknown)

	var decodedLegacy opensplunkv1.KnowledgeValidationResult
	if err := proto.Unmarshal(legacyWire, &decodedLegacy); err != nil {
		t.Fatalf("unmarshal pre-route draft result: %v", err)
	}
	if len(decodedLegacy.GetDependencies()) != 0 || len(decodedLegacy.GetDiagnostics()) != 0 {
		t.Fatal("reserved draft fields were reinterpreted as candidate projections")
	}
	if !bytes.Equal(decodedLegacy.ProtoReflect().GetUnknown(), legacyTopLevelUnknown) {
		t.Fatalf("top-level draft unknown fields = %x, want %x", decodedLegacy.ProtoReflect().GetUnknown(), legacyTopLevelUnknown)
	}
	if decodedLegacy.GetResources() == nil ||
		!bytes.Equal(decodedLegacy.GetResources().ProtoReflect().GetUnknown(), legacyResourceUnknown) {
		t.Fatalf("resource draft unknown field = %x, want %x", decodedLegacy.GetResources().ProtoReflect().GetUnknown(), legacyResourceUnknown)
	}
	roundTrip, err := proto.MarshalOptions{Deterministic: true}.Marshal(&decodedLegacy)
	if err != nil {
		t.Fatalf("remarshal pre-route draft result: %v", err)
	}
	var decodedAgain opensplunkv1.KnowledgeValidationResult
	if err := proto.Unmarshal(roundTrip, &decodedAgain); err != nil {
		t.Fatalf("re-unmarshal pre-route draft result: %v", err)
	}
	if !bytes.Equal(decodedAgain.ProtoReflect().GetUnknown(), legacyTopLevelUnknown) ||
		decodedAgain.GetResources() == nil ||
		!bytes.Equal(decodedAgain.GetResources().ProtoReflect().GetUnknown(), legacyResourceUnknown) {
		t.Fatal("reserved pre-route draft fields did not survive a Go wire round trip")
	}

	result := &opensplunkv1.KnowledgeValidationResult{
		Dependencies: []*opensplunkv1.KnowledgeValidationDependency{{
			Target: &opensplunkv1.KnowledgeManagementObjectVersionIdentity{
				KnowledgeObjectId: "ko-target",
				Version:           7,
			},
			Role: opensplunkv1.KnowledgeDependencyRole_KNOWLEDGE_DEPENDENCY_ROLE_FIELD_INPUT,
		}},
		Diagnostics: []*opensplunkv1.KnowledgeValidationDiagnostic{{
			FieldPath: "field_extraction.regex.pattern",
			Diagnostic: &opensplunkv1.Diagnostic{
				Code:     "SPL_EXAMPLE",
				Severity: opensplunkv1.DiagnosticSeverity_DIAGNOSTIC_SEVERITY_WARNING,
				Message:  "example",
			},
		}},
		FieldViolationsTruncated: true,
		DiagnosticsTruncated:     true,
	}
	resultWire, err := proto.MarshalOptions{Deterministic: true}.Marshal(result)
	if err != nil {
		t.Fatalf("marshal candidate projections: %v", err)
	}
	if got := validationContractWireFieldNumbers(t, resultWire); !slices.Equal(got, []protowire.Number{9, 10, 11, 12}) {
		t.Fatalf("candidate projection wire fields = %v, want append-only [9 10 11 12]", got)
	}
}

func TestKnowledgeValidationContractPinsAggregateTextBudgetBoundaries(t *testing.T) {
	t.Parallel()

	violations := make([]*opensplunkv1.FieldViolation, 64)
	for index := range violations {
		violations[index] = &opensplunkv1.FieldViolation{
			FieldPath: fmt.Sprintf("p%03d", index),
			Code:      "C",
			Message:   strings.Repeat("v", 4091),
		}
	}
	retained, textBytes, truncated := validationContractFieldViolationPrefix(violations)
	if retained != len(violations) || textBytes != 256<<10 || truncated {
		t.Fatalf(
			"exact field-violation budget = retained:%d bytes:%d truncated:%t, want 64/%d/false",
			retained,
			textBytes,
			truncated,
			256<<10,
		)
	}
	violations[len(violations)-1].Message += "x"
	retained, textBytes, truncated = validationContractFieldViolationPrefix(violations)
	if retained != len(violations)-1 || textBytes != (len(violations)-1)*4096 || !truncated {
		t.Fatalf(
			"field-violation budget +1 = retained:%d bytes:%d truncated:%t, want 63/%d/true",
			retained,
			textBytes,
			truncated,
			(len(violations)-1)*4096,
		)
	}

	diagnostics := make([]*opensplunkv1.KnowledgeValidationDiagnostic, 192)
	for index := range diagnostics {
		diagnostics[index] = &opensplunkv1.KnowledgeValidationDiagnostic{
			FieldPath: fmt.Sprintf("p%03d", index),
			Diagnostic: &opensplunkv1.Diagnostic{
				Code:        "C",
				Severity:    opensplunkv1.DiagnosticSeverity_DIAGNOSTIC_SEVERITY_WARNING,
				Message:     strings.Repeat("d", 4091),
				Suggestions: []string{},
			},
		}
	}
	retained, textBytes, truncated = validationContractDiagnosticPrefix(diagnostics)
	if retained != len(diagnostics) || textBytes != 768<<10 || truncated {
		t.Fatalf(
			"exact diagnostic budget = retained:%d bytes:%d truncated:%t, want 192/%d/false",
			retained,
			textBytes,
			truncated,
			768<<10,
		)
	}
	diagnostics[len(diagnostics)-1].Diagnostic.Message += "x"
	retained, textBytes, truncated = validationContractDiagnosticPrefix(diagnostics)
	if retained != len(diagnostics)-1 || textBytes != (len(diagnostics)-1)*4096 || !truncated {
		t.Fatalf(
			"diagnostic budget +1 = retained:%d bytes:%d truncated:%t, want 191/%d/true",
			retained,
			textBytes,
			truncated,
			(len(diagnostics)-1)*4096,
		)
	}

	withSuggestions := []*opensplunkv1.KnowledgeValidationDiagnostic{{
		FieldPath: "p",
		Diagnostic: &opensplunkv1.Diagnostic{
			Code:        "C",
			Severity:    opensplunkv1.DiagnosticSeverity_DIAGNOSTIC_SEVERITY_WARNING,
			Message:     "M",
			Suggestions: []string{"one", "é"},
		},
	}}
	retained, textBytes, truncated = validationContractDiagnosticPrefix(withSuggestions)
	if retained != 1 || textBytes != 8 || truncated {
		t.Fatalf("diagnostic suggestion charge = retained:%d bytes:%d truncated:%t, want 1/8/false", retained, textBytes, truncated)
	}

	duplicateViolation := []*opensplunkv1.FieldViolation{violations[0], proto.Clone(violations[0]).(*opensplunkv1.FieldViolation)}
	retained, _, truncated = validationContractFieldViolationPrefix(duplicateViolation)
	if retained != 1 || truncated {
		t.Fatalf("duplicate field violations = retained:%d truncated:%t, want 1/false", retained, truncated)
	}

	warningFill := make([]*opensplunkv1.KnowledgeValidationDiagnostic, 192)
	for index := range warningFill {
		warningFill[index] = &opensplunkv1.KnowledgeValidationDiagnostic{
			FieldPath: fmt.Sprintf("w%03d", index),
			Diagnostic: &opensplunkv1.Diagnostic{
				Code:     "W",
				Severity: opensplunkv1.DiagnosticSeverity_DIAGNOSTIC_SEVERITY_WARNING,
				Message:  strings.Repeat("w", 4091),
			},
		}
	}
	errorDiagnostic := &opensplunkv1.KnowledgeValidationDiagnostic{
		FieldPath: "z",
		Diagnostic: &opensplunkv1.Diagnostic{
			Code:     "E",
			Severity: opensplunkv1.DiagnosticSeverity_DIAGNOSTIC_SEVERITY_ERROR,
			Message:  "invalid",
		},
	}
	warningFill = append(
		warningFill,
		errorDiagnostic,
		proto.Clone(errorDiagnostic).(*opensplunkv1.KnowledgeValidationDiagnostic),
	)
	canonical := validationContractCanonicalDiagnostics(warningFill)
	if len(canonical) != 193 || canonical[0].GetDiagnostic().GetSeverity() != opensplunkv1.DiagnosticSeverity_DIAGNOSTIC_SEVERITY_ERROR {
		t.Fatalf("ERROR-first deduplicated diagnostics = len:%d first:%v", len(canonical), canonical[0].GetDiagnostic().GetSeverity())
	}
	retained, textBytes, truncated = validationContractDiagnosticPrefix(warningFill)
	if retained != 192 || textBytes != 191*4096+9 || !truncated {
		t.Fatalf(
			"warning-filled invalid diagnostic prefix = retained:%d bytes:%d truncated:%t, want 192/%d/true",
			retained,
			textBytes,
			truncated,
			191*4096+9,
		)
	}

	tinyViolations := make([]*opensplunkv1.FieldViolation, validationContractMaximumIssues+1)
	for index := range tinyViolations {
		tinyViolations[index] = &opensplunkv1.FieldViolation{
			FieldPath: fmt.Sprintf("c%03d", index),
			Code:      "C",
			Message:   "m",
		}
	}
	retained, textBytes, truncated = validationContractFieldViolationPrefix(tinyViolations[:validationContractMaximumIssues])
	if retained != validationContractMaximumIssues || textBytes >= validationContractFieldViolationTextBudget || truncated {
		t.Fatalf("256 tiny violations = retained:%d bytes:%d truncated:%t", retained, textBytes, truncated)
	}
	retained, textBytes, truncated = validationContractFieldViolationPrefix(tinyViolations)
	if retained != validationContractMaximumIssues || textBytes >= validationContractFieldViolationTextBudget || !truncated {
		t.Fatalf("257 tiny violations = retained:%d bytes:%d truncated:%t", retained, textBytes, truncated)
	}

	tinyDiagnostics := make([]*opensplunkv1.KnowledgeValidationDiagnostic, validationContractMaximumIssues+1)
	for index := range tinyDiagnostics {
		tinyDiagnostics[index] = &opensplunkv1.KnowledgeValidationDiagnostic{
			FieldPath: fmt.Sprintf("c%03d", index),
			Diagnostic: &opensplunkv1.Diagnostic{
				Code:     "C",
				Severity: opensplunkv1.DiagnosticSeverity_DIAGNOSTIC_SEVERITY_WARNING,
				Message:  "m",
			},
		}
	}
	retained, textBytes, truncated = validationContractDiagnosticPrefix(tinyDiagnostics[:validationContractMaximumIssues])
	if retained != validationContractMaximumIssues || textBytes >= validationContractDiagnosticTextBudget || truncated {
		t.Fatalf("256 tiny diagnostics = retained:%d bytes:%d truncated:%t", retained, textBytes, truncated)
	}
	retained, textBytes, truncated = validationContractDiagnosticPrefix(tinyDiagnostics)
	if retained != validationContractMaximumIssues || textBytes >= validationContractDiagnosticTextBudget || !truncated {
		t.Fatalf("257 tiny diagnostics = retained:%d bytes:%d truncated:%t", retained, textBytes, truncated)
	}
}

func TestKnowledgeValidationContractPinsExpectedVersionSignedBoundary(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name    string
		version uint64
		allowed bool
	}{
		{name: "zero", version: 0, allowed: false},
		{name: "one", version: 1, allowed: true},
		{name: "MaxInt64", version: validationContractMaximumExpectedVersion, allowed: true},
		{name: "MaxInt64 plus one", version: validationContractMaximumExpectedVersion + 1, allowed: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := validationContractExpectedVersionAllowed(test.version); got != test.allowed {
				t.Fatalf("expected-version authority for %d = %t, want %t", test.version, got, test.allowed)
			}

			objectID := "ko-version-boundary"
			version := test.version
			request := &opensplunkv1.ValidateKnowledgeObjectRequest{
				Definition:        validationContractMinimalDefinition(),
				KnowledgeObjectId: &objectID,
				ExpectedVersion:   &version,
				UpdateMask:        &fieldmaskpb.FieldMask{Paths: []string{"name"}},
				Intent:            opensplunkv1.KnowledgeValidationIntent_KNOWLEDGE_VALIDATION_INTENT_INACTIVE_STORAGE,
			}
			wire, err := proto.MarshalOptions{Deterministic: true}.Marshal(request)
			if err != nil {
				t.Fatalf("marshal expected-version boundary request: %v", err)
			}
			var decoded opensplunkv1.ValidateKnowledgeObjectRequest
			if err := proto.Unmarshal(wire, &decoded); err != nil {
				t.Fatalf("unmarshal expected-version boundary request: %v", err)
			}
			if decoded.ExpectedVersion == nil || *decoded.ExpectedVersion != test.version {
				t.Fatalf("expected-version boundary wire = %v, want present %d", decoded.ExpectedVersion, test.version)
			}
		})
	}
}

const (
	validationContractMaximumIssues            = 256
	validationContractFieldViolationTextBudget = 256 << 10
	validationContractDiagnosticTextBudget     = 768 << 10
	validationContractMaximumExpectedVersion   = uint64(1<<63 - 1)
)

func validationContractExpectedVersionAllowed(version uint64) bool {
	return version >= 1 && version <= validationContractMaximumExpectedVersion
}

func validationContractMinimalDefinition() *opensplunkv1.KnowledgeObjectDefinition {
	return &opensplunkv1.KnowledgeObjectDefinition{
		AppId:        "app_AAAAAAAAAAAAAAAAAAAAAA",
		Name:         "revenue",
		SharingScope: opensplunkv1.SharingScope_SHARING_SCOPE_PRIVATE,
		Selector: &opensplunkv1.KnowledgeSelector{
			IndexPatterns: []*opensplunkv1.KnowledgeSelectorPattern{{
				MatchKind: opensplunkv1.KnowledgeSelectorMatchKind_KNOWLEDGE_SELECTOR_MATCH_KIND_EXACT,
				Value:     "main",
			}},
		},
		Body: &opensplunkv1.KnowledgeObjectDefinition_FieldAlias{
			FieldAlias: &opensplunkv1.FieldAliasDefinition{
				SourceField:       "source.value",
				DestinationField:  "derived.value",
				OverwriteBehavior: opensplunkv1.KnowledgeOverwriteBehavior_KNOWLEDGE_OVERWRITE_BEHAVIOR_PRESERVE_EXISTING,
			},
		},
	}
}

func validationContractFieldViolationPrefix(input []*opensplunkv1.FieldViolation) (int, int, bool) {
	canonical := validationContractCanonicalFieldViolations(input)
	textBytes := 0
	for index, violation := range canonical {
		if index == validationContractMaximumIssues {
			return index, textBytes, true
		}
		charge := len(violation.GetFieldPath()) + len(violation.GetCode()) + len(violation.GetMessage())
		if charge > validationContractFieldViolationTextBudget-textBytes {
			return index, textBytes, true
		}
		textBytes += charge
	}
	return len(canonical), textBytes, false
}

func validationContractDiagnosticPrefix(input []*opensplunkv1.KnowledgeValidationDiagnostic) (int, int, bool) {
	canonical := validationContractCanonicalDiagnostics(input)
	textBytes := 0
	for index, diagnostic := range canonical {
		if index == validationContractMaximumIssues {
			return index, textBytes, true
		}
		value := diagnostic.GetDiagnostic()
		charge := len(diagnostic.GetFieldPath()) + len(value.GetCode()) + len(value.GetMessage())
		for _, suggestion := range value.GetSuggestions() {
			charge += len(suggestion)
		}
		if charge > validationContractDiagnosticTextBudget-textBytes {
			return index, textBytes, true
		}
		textBytes += charge
	}
	return len(canonical), textBytes, false
}

func validationContractCanonicalFieldViolations(input []*opensplunkv1.FieldViolation) []*opensplunkv1.FieldViolation {
	canonical := slices.Clone(input)
	slices.SortFunc(canonical, func(left, right *opensplunkv1.FieldViolation) int {
		if order := cmp.Compare(left.GetFieldPath(), right.GetFieldPath()); order != 0 {
			return order
		}
		if order := cmp.Compare(left.GetCode(), right.GetCode()); order != 0 {
			return order
		}
		return cmp.Compare(left.GetMessage(), right.GetMessage())
	})
	return slices.CompactFunc(canonical, func(left, right *opensplunkv1.FieldViolation) bool {
		return proto.Equal(left, right)
	})
}

func validationContractCanonicalDiagnostics(input []*opensplunkv1.KnowledgeValidationDiagnostic) []*opensplunkv1.KnowledgeValidationDiagnostic {
	canonical := slices.Clone(input)
	slices.SortFunc(canonical, validationContractCompareDiagnostics)
	return slices.CompactFunc(canonical, func(left, right *opensplunkv1.KnowledgeValidationDiagnostic) bool {
		return proto.Equal(left, right)
	})
}

func validationContractCompareDiagnostics(left, right *opensplunkv1.KnowledgeValidationDiagnostic) int {
	leftValue := left.GetDiagnostic()
	rightValue := right.GetDiagnostic()
	if order := cmp.Compare(
		validationContractDiagnosticSeverityRank(leftValue.GetSeverity()),
		validationContractDiagnosticSeverityRank(rightValue.GetSeverity()),
	); order != 0 {
		return order
	}
	if order := cmp.Compare(left.GetFieldPath(), right.GetFieldPath()); order != 0 {
		return order
	}
	leftRange := leftValue.GetSourceRange()
	rightRange := rightValue.GetSourceRange()
	if (leftRange != nil) != (rightRange != nil) {
		if leftRange == nil {
			return -1
		}
		return 1
	}
	if leftRange != nil {
		for _, positions := range [][2]*opensplunkv1.SourcePosition{
			{leftRange.GetStart(), rightRange.GetStart()},
			{leftRange.GetEnd(), rightRange.GetEnd()},
		} {
			if order := cmp.Compare(positions[0].GetByteOffset(), positions[1].GetByteOffset()); order != 0 {
				return order
			}
		}
	}
	if order := cmp.Compare(leftValue.GetCode(), rightValue.GetCode()); order != 0 {
		return order
	}
	if order := cmp.Compare(leftValue.GetMessage(), rightValue.GetMessage()); order != 0 {
		return order
	}
	if leftRange != nil {
		for _, positions := range [][2]*opensplunkv1.SourcePosition{
			{leftRange.GetStart(), rightRange.GetStart()},
			{leftRange.GetEnd(), rightRange.GetEnd()},
		} {
			if order := cmp.Compare(positions[0].GetLine(), positions[1].GetLine()); order != 0 {
				return order
			}
			if order := cmp.Compare(positions[0].GetColumn(), positions[1].GetColumn()); order != 0 {
				return order
			}
		}
	}
	leftSuggestions := leftValue.GetSuggestions()
	rightSuggestions := rightValue.GetSuggestions()
	for index := 0; index < min(len(leftSuggestions), len(rightSuggestions)); index++ {
		if order := cmp.Compare(leftSuggestions[index], rightSuggestions[index]); order != 0 {
			return order
		}
	}
	return cmp.Compare(len(leftSuggestions), len(rightSuggestions))
}

func validationContractDiagnosticSeverityRank(severity opensplunkv1.DiagnosticSeverity) int {
	switch severity {
	case opensplunkv1.DiagnosticSeverity_DIAGNOSTIC_SEVERITY_ERROR:
		return 0
	case opensplunkv1.DiagnosticSeverity_DIAGNOSTIC_SEVERITY_WARNING:
		return 1
	case opensplunkv1.DiagnosticSeverity_DIAGNOSTIC_SEVERITY_INFO:
		return 2
	default:
		return 3
	}
}

func validationContractRequireField(
	t *testing.T,
	message protoreflect.MessageDescriptor,
	name protoreflect.Name,
	number protoreflect.FieldNumber,
	kind protoreflect.Kind,
	typeName protoreflect.FullName,
) protoreflect.FieldDescriptor {
	t.Helper()
	field := message.Fields().ByName(name)
	if field == nil || field.Number() != number || field.Kind() != kind {
		t.Fatalf("%s.%s = %v, want field %d with kind %s", message.Name(), name, field, number, kind)
	}
	if typeName != "" {
		var got protoreflect.FullName
		switch kind {
		case protoreflect.MessageKind:
			if field.Message() != nil {
				got = field.Message().FullName()
			}
		case protoreflect.EnumKind:
			if field.Enum() != nil {
				got = field.Enum().FullName()
			}
		}
		if got != typeName {
			t.Fatalf("%s.%s type = %s, want %s", message.Name(), name, got, typeName)
		}
	}
	return field
}

func validationContractNumberReserved(message protoreflect.MessageDescriptor, number protoreflect.FieldNumber) bool {
	ranges := message.ReservedRanges()
	for index := 0; index < ranges.Len(); index++ {
		reserved := ranges.Get(index)
		if number >= reserved[0] && number < reserved[1] {
			return true
		}
	}
	return false
}

func validationContractNameReserved(message protoreflect.MessageDescriptor, name protoreflect.Name) bool {
	names := message.ReservedNames()
	for index := 0; index < names.Len(); index++ {
		if names.Get(index) == name {
			return true
		}
	}
	return false
}

func validationContractRequireProtoComments(t *testing.T, anchor string, fragments ...string) {
	t.Helper()
	source, err := os.ReadFile("proto/open_splunk/v1/knowledge_api.proto")
	if err != nil {
		t.Fatalf("read knowledge validation protobuf source: %v", err)
	}
	lines := strings.Split(string(source), "\n")
	anchorIndex := -1
	for index, line := range lines {
		if strings.TrimSpace(line) != anchor {
			continue
		}
		if anchorIndex >= 0 {
			t.Fatalf("protobuf comment anchor %q is not unique", anchor)
		}
		anchorIndex = index
	}
	if anchorIndex < 0 {
		t.Fatalf("protobuf comment anchor %q is missing", anchor)
	}
	commentStart := anchorIndex
	for commentStart > 0 && strings.HasPrefix(strings.TrimSpace(lines[commentStart-1]), "//") {
		commentStart--
	}
	if commentStart == anchorIndex {
		t.Fatalf("protobuf anchor %q has no directly attached leading comments", anchor)
	}
	comments := strings.Join(lines[commentStart:anchorIndex], "\n")
	normalizedComments := validationContractNormalizeProtoComments(lines[commentStart:anchorIndex])
	for _, fragment := range fragments {
		normalizedFragment := strings.Join(strings.Fields(fragment), " ")
		if !strings.Contains(normalizedComments, normalizedFragment) {
			t.Errorf("protobuf comments attached to %q do not contain %q:\n%s", anchor, fragment, comments)
		}
	}
}

func validationContractRequireNestedProtoComments(
	t *testing.T,
	containerAnchor string,
	fieldAnchor string,
	fragments ...string,
) {
	t.Helper()
	source, err := os.ReadFile("proto/open_splunk/v1/knowledge_api.proto")
	if err != nil {
		t.Fatalf("read knowledge validation protobuf source: %v", err)
	}
	lines := strings.Split(string(source), "\n")
	containerIndex := -1
	for index, line := range lines {
		if strings.TrimSpace(line) == containerAnchor {
			if containerIndex >= 0 {
				t.Fatalf("protobuf container anchor %q is not unique", containerAnchor)
			}
			containerIndex = index
		}
	}
	if containerIndex < 0 {
		t.Fatalf("protobuf container anchor %q is missing", containerAnchor)
	}
	fieldIndex := -1
	for index := containerIndex + 1; index < len(lines); index++ {
		trimmed := strings.TrimSpace(lines[index])
		if trimmed == "}" {
			break
		}
		if trimmed == fieldAnchor {
			if fieldIndex >= 0 {
				t.Fatalf("protobuf field anchor %q is not unique within %q", fieldAnchor, containerAnchor)
			}
			fieldIndex = index
		}
	}
	if fieldIndex < 0 {
		t.Fatalf("protobuf field anchor %q is missing within %q", fieldAnchor, containerAnchor)
	}
	commentStart := fieldIndex
	for commentStart > containerIndex+1 && strings.HasPrefix(strings.TrimSpace(lines[commentStart-1]), "//") {
		commentStart--
	}
	if commentStart == fieldIndex {
		t.Fatalf("protobuf field anchor %q within %q has no directly attached comments", fieldAnchor, containerAnchor)
	}
	comments := strings.Join(lines[commentStart:fieldIndex], "\n")
	normalizedComments := validationContractNormalizeProtoComments(lines[commentStart:fieldIndex])
	for _, fragment := range fragments {
		normalizedFragment := strings.Join(strings.Fields(fragment), " ")
		if !strings.Contains(normalizedComments, normalizedFragment) {
			t.Errorf(
				"protobuf comments attached to %q within %q do not contain %q:\n%s",
				fieldAnchor,
				containerAnchor,
				fragment,
				comments,
			)
		}
	}
}

func validationContractNormalizeProtoComments(lines []string) string {
	values := make([]string, 0, len(lines))
	for _, line := range lines {
		value := strings.TrimSpace(line)
		value = strings.TrimSpace(strings.TrimPrefix(value, "//"))
		if value != "" {
			values = append(values, value)
		}
	}
	return strings.Join(values, " ")
}

func validationContractWireFieldNumbers(t *testing.T, wire []byte) []protowire.Number {
	t.Helper()
	result := make([]protowire.Number, 0, 8)
	for len(wire) > 0 {
		number, wireType, tagBytes := protowire.ConsumeTag(wire)
		if tagBytes < 0 {
			t.Fatalf("consume protobuf tag: %v", protowire.ParseError(tagBytes))
		}
		wire = wire[tagBytes:]
		valueBytes := protowire.ConsumeFieldValue(number, wireType, wire)
		if valueBytes < 0 {
			t.Fatalf("consume protobuf field %d: %v", number, protowire.ParseError(valueBytes))
		}
		result = append(result, number)
		wire = wire[valueBytes:]
	}
	return result
}
