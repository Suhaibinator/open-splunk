package knowledgevalidation

import (
	"context"
	"testing"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
)

func aliasDefinition(name string) *opensplunkv1.KnowledgeObjectDefinition {
	return &opensplunkv1.KnowledgeObjectDefinition{
		AppId:        "app-a",
		Name:         name,
		SharingScope: opensplunkv1.SharingScope_SHARING_SCOPE_PRIVATE,
		Body: &opensplunkv1.KnowledgeObjectDefinition_FieldAlias{
			FieldAlias: &opensplunkv1.FieldAliasDefinition{
				SourceField:      "source_value",
				DestinationField: "derived_value",
			},
		},
	}
}

func calculatedDefinition(name, expression string) *opensplunkv1.KnowledgeObjectDefinition {
	return &opensplunkv1.KnowledgeObjectDefinition{
		AppId:        "app-a",
		Name:         name,
		SharingScope: opensplunkv1.SharingScope_SHARING_SCOPE_PRIVATE,
		Body: &opensplunkv1.KnowledgeObjectDefinition_CalculatedField{
			CalculatedField: &opensplunkv1.CalculatedFieldDefinition{
				DestinationField: "derived_value",
				Expression:       expression,
			},
		},
	}
}

func regexDefinition(name, pattern string, outputs ...string) *opensplunkv1.KnowledgeObjectDefinition {
	return &opensplunkv1.KnowledgeObjectDefinition{
		AppId:        "app-a",
		Name:         name,
		SharingScope: opensplunkv1.SharingScope_SHARING_SCOPE_PRIVATE,
		Body: &opensplunkv1.KnowledgeObjectDefinition_FieldExtraction{
			FieldExtraction: &opensplunkv1.FieldExtractionDefinition{
				InputField: "_raw",
				Extraction: &opensplunkv1.FieldExtractionDefinition_Regex{
					Regex: &opensplunkv1.RegexFieldExtractionDefinition{
						Pattern:      pattern,
						OutputFields: append([]string(nil), outputs...),
					},
				},
			},
		},
	}
}

func jsonDefinition(name, path string) *opensplunkv1.KnowledgeObjectDefinition {
	return &opensplunkv1.KnowledgeObjectDefinition{
		AppId:        "app-a",
		Name:         name,
		SharingScope: opensplunkv1.SharingScope_SHARING_SCOPE_PRIVATE,
		Body: &opensplunkv1.KnowledgeObjectDefinition_FieldExtraction{
			FieldExtraction: &opensplunkv1.FieldExtractionDefinition{
				InputField: "_raw",
				Extraction: &opensplunkv1.FieldExtractionDefinition_Json{
					Json: &opensplunkv1.JsonFieldExtractionDefinition{
						Path:        path,
						OutputField: "derived_value",
					},
				},
			},
		},
	}
}

func mustResultProto(t *testing.T, result Result) *opensplunkv1.KnowledgeValidationResult {
	t.Helper()
	value, err := result.Proto(context.Background())
	if err != nil {
		t.Fatalf("Result.Proto: %v", err)
	}
	return value
}

func mustActiveCandidate(t *testing.T, definition *opensplunkv1.KnowledgeObjectDefinition) ActiveCandidate {
	t.Helper()
	preparation, err := PrepareActive(context.Background(), definition)
	if err != nil {
		t.Fatalf("PrepareActive: %v", err)
	}
	if invalid, ok := preparation.Invalid(); ok {
		t.Fatalf("PrepareActive returned invalid result: %+v", mustResultProto(t, invalid))
	}
	candidate, ok := preparation.Candidate()
	if !ok {
		t.Fatal("PrepareActive returned neither candidate nor invalid result")
	}
	return candidate
}
