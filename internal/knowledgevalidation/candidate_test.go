package knowledgevalidation

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/knowledgedefinition"
	"github.com/Suhaibinator/open-splunk/internal/knowledgeprogram"
)

func TestMissingDefinitionIsEnvelopeFailure(t *testing.T) {
	if _, err := BuildInactive(context.Background(), nil); !errors.Is(err, ErrEnvelope) {
		t.Fatalf("BuildInactive(nil) error = %v", err)
	}
	if _, err := PrepareActive(context.Background(), nil); !errors.Is(err, ErrEnvelope) {
		t.Fatalf("PrepareActive(nil) error = %v", err)
	}
}

func TestDefinitionIssuesBecomeClosedInvalidFieldViolations(t *testing.T) {
	unknown := aliasDefinition("alias-unknown-input")
	unknown.GetFieldAlias().ProtoReflect().SetUnknown([]byte{0xa0, 0x06, 0x01})
	tooManyOutputs := make([]string, knowledgedefinition.MaximumFieldExtractionOutputs+1)
	for index := range tooManyOutputs {
		tooManyOutputs[index] = fmt.Sprintf("field_%02d", index)
	}
	tests := []struct {
		name       string
		definition *opensplunkv1.KnowledgeObjectDefinition
		objectType opensplunkv1.KnowledgeObjectType
		code       string
		message    string
	}{
		{
			name:       "missing body",
			definition: &opensplunkv1.KnowledgeObjectDefinition{},
			objectType: opensplunkv1.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_UNSPECIFIED,
			code:       "KNOWLEDGE_DEFINITION_INVALID",
			message:    "candidate definition field is invalid",
		},
		{
			name:       "recursive unknown field",
			definition: unknown,
			objectType: opensplunkv1.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_FIELD_ALIAS,
			code:       "KNOWLEDGE_DEFINITION_UNKNOWN_FIELD",
			message:    "candidate definition contains an unknown protobuf field",
		},
		{
			name:       "preflight resource limit",
			definition: regexDefinition("regex-too-many-outputs", `(?P<value>x)`, tooManyOutputs...),
			objectType: opensplunkv1.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_FIELD_EXTRACTION,
			code:       "KNOWLEDGE_DEFINITION_RESOURCE_LIMIT",
			message:    "candidate definition exceeds a resource limit",
		},
		{
			name: "known alias body",
			definition: &opensplunkv1.KnowledgeObjectDefinition{
				Body: &opensplunkv1.KnowledgeObjectDefinition_FieldAlias{FieldAlias: &opensplunkv1.FieldAliasDefinition{}},
			},
			objectType: opensplunkv1.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_FIELD_ALIAS,
			code:       "KNOWLEDGE_DEFINITION_INVALID",
			message:    "candidate definition field is invalid",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result, err := BuildInactive(context.Background(), test.definition)
			if err != nil {
				t.Fatalf("BuildInactive: %v", err)
			}
			value := mustResultProto(t, result)
			if value.GetValid() || value.GetObjectType() != test.objectType || len(value.GetFieldViolations()) != 1 ||
				value.GetFieldViolations()[0].GetCode() != test.code ||
				value.GetFieldViolations()[0].GetMessage() != test.message ||
				value.GetNormalizedDefinition() != nil || value.DefinitionSha256 != nil ||
				value.GetResources() != nil || len(value.GetDependencies()) != 0 {
				t.Fatalf("invalid result = %+v", value)
			}
		})
	}
}

func TestInactiveDoesNotCompilePublicationSemantics(t *testing.T) {
	definition := calculatedDefinition("calculated-inactive", "mystery(host)")
	result, err := BuildInactive(context.Background(), definition)
	if err != nil {
		t.Fatalf("BuildInactive: %v", err)
	}
	value := mustResultProto(t, result)
	if !value.GetValid() || value.GetNormalizedDefinition() == nil || value.GetResources() == nil ||
		len(value.GetDependencies()) != 0 || !zeroPublicationResources(value.GetResources()) {
		t.Fatalf("inactive result = %+v", value)
	}

	preparation, err := PrepareActive(context.Background(), definition)
	if err != nil {
		t.Fatalf("PrepareActive: %v", err)
	}
	invalid, ok := preparation.Invalid()
	if !ok {
		t.Fatal("active preparation accepted unsupported calculated function")
	}
	active := mustResultProto(t, invalid)
	if active.GetValid() || len(active.GetDiagnostics()) != 1 ||
		active.GetDiagnostics()[0].GetDiagnostic().GetCode() != "SPL_UNSUPPORTED_EVAL_FUNCTION" {
		t.Fatalf("active invalid result = %+v", active)
	}
}

func TestCalculatedRangesRebaseToSubmittedScalar(t *testing.T) {
	t.Run("token with unicode and leading trim", func(t *testing.T) {
		raw := " \n\tcoalesce(\"😀\", mystery(host)) \r\n"
		preparation, err := PrepareActive(context.Background(), calculatedDefinition("calculated-token", raw))
		if err != nil {
			t.Fatal(err)
		}
		invalid, ok := preparation.Invalid()
		if !ok {
			t.Fatal("invalid expression compiled")
		}
		diagnostic := mustResultProto(t, invalid).GetDiagnostics()[0].GetDiagnostic()
		sourceRange := diagnostic.GetSourceRange()
		if sourceRange == nil {
			t.Fatal("diagnostic range is absent")
		}
		start, end := sourceRange.GetStart().GetByteOffset(), sourceRange.GetEnd().GetByteOffset()
		if raw[start:end] != "mystery" {
			t.Fatalf("rebased range %d:%d selects %q", start, end, raw[start:end])
		}
		wantStart, err := sourcePosition(context.Background(), raw, start)
		if err != nil || sourceRange.GetStart().GetByteOffset() != wantStart.GetByteOffset() ||
			sourceRange.GetStart().GetLine() != wantStart.GetLine() ||
			sourceRange.GetStart().GetColumn() != wantStart.GetColumn() {
			t.Fatalf("start position = %+v, want %+v (%v)", sourceRange.GetStart(), wantStart, err)
		}
	})

	t.Run("canonical EOF maps after trailing trim", func(t *testing.T) {
		raw := "\n\tlower(host \r\n"
		preparation, err := PrepareActive(context.Background(), calculatedDefinition("calculated-eof", raw))
		if err != nil {
			t.Fatal(err)
		}
		invalid, ok := preparation.Invalid()
		if !ok {
			t.Fatal("unterminated expression compiled")
		}
		sourceRange := mustResultProto(t, invalid).GetDiagnostics()[0].GetDiagnostic().GetSourceRange()
		if sourceRange == nil || sourceRange.GetStart().GetByteOffset() != uint64(len(raw)) ||
			sourceRange.GetEnd().GetByteOffset() != uint64(len(raw)) {
			t.Fatalf("EOF range = %+v, raw bytes = %d", sourceRange, len(raw))
		}
	})

	t.Run("boolean range excludes trim", func(t *testing.T) {
		raw := " \tisnull(host)\r\n"
		preparation, err := PrepareActive(context.Background(), calculatedDefinition("calculated-boolean", raw))
		if err != nil {
			t.Fatal(err)
		}
		invalid, ok := preparation.Invalid()
		if !ok {
			t.Fatal("Boolean expression compiled")
		}
		diagnostic := mustResultProto(t, invalid).GetDiagnostics()[0].GetDiagnostic()
		sourceRange := diagnostic.GetSourceRange()
		start, end := sourceRange.GetStart().GetByteOffset(), sourceRange.GetEnd().GetByteOffset()
		if diagnostic.GetCode() != string(knowledgeprogram.IssueCodeCalculatedBoolean) || raw[start:end] != "isnull(host)" {
			t.Fatalf("Boolean diagnostic = %+v, selected %q", diagnostic, raw[start:end])
		}
	})
}

func TestActiveCandidateAndResultAreDetached(t *testing.T) {
	definition := aliasDefinition("alias-detached")
	candidate := mustActiveCandidate(t, definition)
	definition.Name = "mutated-input"
	normalized, digest, err := candidate.Normalized(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if normalized.GetName() != "alias-detached" || digest == [32]byte{} {
		t.Fatalf("normalized authority = %q/%x", normalized.GetName(), digest)
	}
	normalized.Name = "mutated-output"
	again, _, err := candidate.Normalized(context.Background())
	if err != nil || again.GetName() != "alias-detached" {
		t.Fatalf("second normalized projection = %q/%v", again.GetName(), err)
	}
	result, err := candidate.BuildValid(context.Background(), ActivePublication{
		Candidate: ExactIdentity{KnowledgeObjectID: "candidate-a", Version: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	first := mustResultProto(t, result)
	first.NormalizedDefinition.Name = "mutated-result"
	second := mustResultProto(t, result)
	if second.GetNormalizedDefinition().GetName() != "alias-detached" {
		t.Fatalf("result aliases prior projection: %q", second.GetNormalizedDefinition().GetName())
	}
}

func TestValidationHonorsCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := BuildInactive(ctx, aliasDefinition("alias-canceled")); !errors.Is(err, context.Canceled) {
		t.Fatalf("BuildInactive canceled error = %v", err)
	}
	if _, err := PrepareActive(ctx, aliasDefinition("alias-canceled")); !errors.Is(err, context.Canceled) {
		t.Fatalf("PrepareActive canceled error = %v", err)
	}
	if _, err := (Result{}).Proto(context.Background()); !errors.Is(err, ErrInvariant) {
		t.Fatalf("zero Result.Proto error = %v", err)
	}
}

func TestAllowedProgramIssueRequiresExactCodePathRangeShape(t *testing.T) {
	calculated := calculatedDefinition("calculated-shape", "lower(host)")
	regex := regexDefinition("regex-shape", `(?P<value>x)`, "value")
	json := jsonDefinition("json-shape", "server.name")
	full := &knowledgeprogram.ScalarRange{EndByteOffset: uint32(len("lower(host)"))}
	tests := []struct {
		name       string
		definition *opensplunkv1.KnowledgeObjectDefinition
		issue      knowledgeprogram.Issue
		want       bool
	}{
		{name: "regex exact", definition: regex, issue: knowledgeprogram.Issue{FieldPath: "field_extraction.regex.pattern", Code: knowledgeprogram.IssueCodeRegexInvalid, Message: "m"}, want: true},
		{name: "regex mismatched body", definition: calculated, issue: knowledgeprogram.Issue{FieldPath: "field_extraction.regex.pattern", Code: knowledgeprogram.IssueCodeRegexInvalid, Message: "m"}},
		{name: "regex with range", definition: regex, issue: knowledgeprogram.Issue{FieldPath: "field_extraction.regex.pattern", Code: knowledgeprogram.IssueCodeRegexInvalid, Message: "m", Range: full}},
		{name: "capture indexed", definition: regex, issue: knowledgeprogram.Issue{FieldPath: "field_extraction.regex.output_fields[0]", Code: knowledgeprogram.IssueCodeRegexCaptureMismatch, Message: "m"}, want: true},
		{name: "capture out of range", definition: regex, issue: knowledgeprogram.Issue{FieldPath: "field_extraction.regex.output_fields[1]", Code: knowledgeprogram.IssueCodeRegexCaptureMismatch, Message: "m"}},
		{name: "capture malformed index", definition: regex, issue: knowledgeprogram.Issue{FieldPath: "field_extraction.regex.output_fields[00]", Code: knowledgeprogram.IssueCodeRegexCaptureMismatch, Message: "m"}},
		{name: "json exact", definition: json, issue: knowledgeprogram.Issue{FieldPath: "field_extraction.json.path", Code: knowledgeprogram.IssueCodeJSONPathInvalid, Message: "m", Range: &knowledgeprogram.ScalarRange{}}, want: true},
		{name: "json mismatched body", definition: calculated, issue: knowledgeprogram.Issue{FieldPath: "field_extraction.json.path", Code: knowledgeprogram.IssueCodeJSONPathInvalid, Message: "m", Range: &knowledgeprogram.ScalarRange{}}},
		{name: "json missing range", definition: json, issue: knowledgeprogram.Issue{FieldPath: "field_extraction.json.path", Code: knowledgeprogram.IssueCodeJSONPathInvalid, Message: "m"}},
		{name: "boolean full", definition: calculated, issue: knowledgeprogram.Issue{FieldPath: "calculated_field.expression", Code: knowledgeprogram.IssueCodeCalculatedBoolean, Message: "m", Range: full}, want: true},
		{name: "boolean partial", definition: calculated, issue: knowledgeprogram.Issue{FieldPath: "calculated_field.expression", Code: knowledgeprogram.IssueCodeCalculatedBoolean, Message: "m", Range: &knowledgeprogram.ScalarRange{EndByteOffset: 1}}},
		{name: "SPL exact", definition: calculated, issue: knowledgeprogram.Issue{FieldPath: "calculated_field.expression", Code: "SPL_EXAMPLE", Message: "m", Range: full}, want: true},
		{name: "SPL mismatched body", definition: regex, issue: knowledgeprogram.Issue{FieldPath: "calculated_field.expression", Code: "SPL_EXAMPLE", Message: "m", Range: full}},
		{name: "SPL wrong path", definition: calculated, issue: knowledgeprogram.Issue{FieldPath: "field_alias.source_field", Code: "SPL_EXAMPLE", Message: "m", Range: full}},
		{name: "empty SPL suffix", definition: calculated, issue: knowledgeprogram.Issue{FieldPath: "calculated_field.expression", Code: "SPL_", Message: "m", Range: full}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := allowedProgramIssue(test.issue, test.definition); got != test.want {
				t.Fatalf("allowedProgramIssue(%+v) = %t, want %t", test.issue, got, test.want)
			}
		})
	}
}

func TestSourceCoordinatesTreatOnlyLFAsNewline(t *testing.T) {
	source := "a\r😀\nb"
	position, err := sourcePosition(context.Background(), source, uint64(strings.Index(source, "b")))
	if err != nil {
		t.Fatal(err)
	}
	if position.GetLine() != 2 || position.GetColumn() != 1 {
		t.Fatalf("position after CR/Unicode/LF = %+v", position)
	}
}
