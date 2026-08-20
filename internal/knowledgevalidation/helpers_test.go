package knowledgevalidation

import (
	"context"
	"testing"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
)

func aliasDefinition(name string) *opensplunk.KnowledgeObjectDefinition {
	return &opensplunk.KnowledgeObjectDefinition{
		AppId:        "app-a",
		Name:         name,
		SharingScope: opensplunk.SharingScope_SHARING_SCOPE_PRIVATE,
		Body: &opensplunk.KnowledgeObjectDefinition_FieldAlias{
			FieldAlias: &opensplunk.FieldAliasDefinition{
				SourceField:      "source_value",
				DestinationField: "derived_value",
			},
		},
	}
}

func calculatedDefinition(name, expression string) *opensplunk.KnowledgeObjectDefinition {
	return &opensplunk.KnowledgeObjectDefinition{
		AppId:        "app-a",
		Name:         name,
		SharingScope: opensplunk.SharingScope_SHARING_SCOPE_PRIVATE,
		Body: &opensplunk.KnowledgeObjectDefinition_CalculatedField{
			CalculatedField: &opensplunk.CalculatedFieldDefinition{
				DestinationField: "derived_value",
				Expression:       expression,
			},
		},
	}
}

func regexDefinition(name string, outputs ...string) *opensplunk.KnowledgeObjectDefinition {
	return &opensplunk.KnowledgeObjectDefinition{
		AppId:        "app-a",
		Name:         name,
		SharingScope: opensplunk.SharingScope_SHARING_SCOPE_PRIVATE,
		Body: &opensplunk.KnowledgeObjectDefinition_FieldExtraction{
			FieldExtraction: &opensplunk.FieldExtractionDefinition{
				InputField: "_raw",
				Extraction: &opensplunk.FieldExtractionDefinition_Regex{
					Regex: &opensplunk.RegexFieldExtractionDefinition{
						Pattern:      `(?P<value>x)`,
						OutputFields: append([]string(nil), outputs...),
					},
				},
			},
		},
	}
}

func jsonDefinition(name, path string) *opensplunk.KnowledgeObjectDefinition {
	return &opensplunk.KnowledgeObjectDefinition{
		AppId:        "app-a",
		Name:         name,
		SharingScope: opensplunk.SharingScope_SHARING_SCOPE_PRIVATE,
		Body: &opensplunk.KnowledgeObjectDefinition_FieldExtraction{
			FieldExtraction: &opensplunk.FieldExtractionDefinition{
				InputField: "_raw",
				Extraction: &opensplunk.FieldExtractionDefinition_Json{
					Json: &opensplunk.JsonFieldExtractionDefinition{
						Path:        path,
						OutputField: "derived_value",
					},
				},
			},
		},
	}
}

func mustResultProto(t *testing.T, result Result) *opensplunk.KnowledgeValidationResult {
	t.Helper()
	value, err := result.Proto(context.Background())
	if err != nil {
		t.Fatalf("Result.Proto: %v", err)
	}
	return value
}

func mustActiveCandidate(t *testing.T, definition *opensplunk.KnowledgeObjectDefinition) ActiveCandidate {
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
