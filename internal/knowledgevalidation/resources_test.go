package knowledgevalidation

import (
	"context"
	"errors"
	"fmt"
	"testing"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
)

func validationDependency(id string, version uint64) *opensplunk.KnowledgeValidationDependency {
	return &opensplunk.KnowledgeValidationDependency{
		Target: &opensplunk.KnowledgeManagementObjectVersionIdentity{
			KnowledgeObjectId: id,
			Version:           version,
		},
		Role: opensplunk.KnowledgeDependencyRole_KNOWLEDGE_DEPENDENCY_ROLE_FIELD_INPUT,
	}
}

func TestActiveDependenciesSortAndDriveResourceFormulas(t *testing.T) {
	candidate := mustActiveCandidate(t, aliasDefinition("alias-dependencies"))
	publication := ActivePublication{
		Candidate: ExactIdentity{KnowledgeObjectID: "candidate-a", Version: 9},
		Dependencies: []*opensplunk.KnowledgeValidationDependency{
			validationDependency("target-b", 2),
			validationDependency("target-a", 3),
		},
	}
	result, err := candidate.BuildValid(context.Background(), publication)
	if err != nil {
		t.Fatal(err)
	}
	value := mustResultProto(t, result)
	if len(value.GetDependencies()) != 2 ||
		value.GetDependencies()[0].GetTarget().GetKnowledgeObjectId() != "target-a" ||
		value.GetDependencies()[1].GetTarget().GetKnowledgeObjectId() != "target-b" {
		t.Fatalf("dependencies are not canonical: %+v", value.GetDependencies())
	}
	resources := value.GetResources()
	if resources.GetDependencyEdges() != 2 || resources.GetDependencyNodes() != 2 ||
		resources.GetGeneratedOperators() != 1 || resources.GetGeneratedFields() != 1 {
		t.Fatalf("active resources = %+v", resources)
	}

	publication.Dependencies[0].Target.KnowledgeObjectId = "mutated-target"
	again := mustResultProto(t, result)
	if again.GetDependencies()[1].GetTarget().GetKnowledgeObjectId() != "target-b" {
		t.Fatal("result aliases caller-owned dependency input")
	}
}

func TestActiveDependenciesRejectDuplicatesAndSelfByObjectID(t *testing.T) {
	candidate := mustActiveCandidate(t, aliasDefinition("alias-dependency-errors"))
	unknownDependency := validationDependency("target-unknown-edge", 1)
	unknownDependency.ProtoReflect().SetUnknown([]byte{0xa0, 0x06, 0x01})
	unknownTarget := validationDependency("target-unknown-identity", 1)
	unknownTarget.Target.ProtoReflect().SetUnknown([]byte{0xa0, 0x06, 0x01})
	tests := []struct {
		name        string
		publication ActivePublication
	}{
		{
			name: "duplicate",
			publication: ActivePublication{
				Candidate: ExactIdentity{KnowledgeObjectID: "candidate-a", Version: 1},
				Dependencies: []*opensplunk.KnowledgeValidationDependency{
					validationDependency("target-a", 1),
					validationDependency("target-a", 1),
				},
			},
		},
		{
			name: "self same version",
			publication: ActivePublication{
				Candidate:    ExactIdentity{KnowledgeObjectID: "candidate-a", Version: 1},
				Dependencies: []*opensplunk.KnowledgeValidationDependency{validationDependency("candidate-a", 1)},
			},
		},
		{
			name: "self different version",
			publication: ActivePublication{
				Candidate:    ExactIdentity{KnowledgeObjectID: "candidate-a", Version: 1},
				Dependencies: []*opensplunk.KnowledgeValidationDependency{validationDependency("candidate-a", 99)},
			},
		},
		{
			name: "wrong role",
			publication: ActivePublication{
				Candidate: ExactIdentity{KnowledgeObjectID: "candidate-a", Version: 1},
				Dependencies: []*opensplunk.KnowledgeValidationDependency{{
					Target: validationDependency("target-a", 1).Target,
					Role:   opensplunk.KnowledgeDependencyRole_KNOWLEDGE_DEPENDENCY_ROLE_LOOKUP_ASSET,
				}},
			},
		},
		{
			name: "zero target version",
			publication: ActivePublication{
				Candidate:    ExactIdentity{KnowledgeObjectID: "candidate-a", Version: 1},
				Dependencies: []*opensplunk.KnowledgeValidationDependency{validationDependency("target-a", 0)},
			},
		},
		{
			name: "dependency unknown field",
			publication: ActivePublication{
				Candidate:    ExactIdentity{KnowledgeObjectID: "candidate-a", Version: 1},
				Dependencies: []*opensplunk.KnowledgeValidationDependency{unknownDependency},
			},
		},
		{
			name: "target unknown field",
			publication: ActivePublication{
				Candidate:    ExactIdentity{KnowledgeObjectID: "candidate-a", Version: 1},
				Dependencies: []*opensplunk.KnowledgeValidationDependency{unknownTarget},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := candidate.BuildValid(context.Background(), test.publication); !errors.Is(err, ErrInvariant) {
				t.Fatalf("BuildValid error = %v", err)
			}
		})
	}
}

func TestActiveDependencyUniqueCap(t *testing.T) {
	candidate := mustActiveCandidate(t, aliasDefinition("alias-dependency-cap"))
	dependencies := make([]*opensplunk.KnowledgeValidationDependency, MaximumDependencies+1)
	for index := range dependencies {
		dependencies[index] = validationDependency(fmt.Sprintf("target-%04d", index), 1)
	}
	publication := ActivePublication{
		Candidate:    ExactIdentity{KnowledgeObjectID: "candidate-a", Version: 1},
		Dependencies: dependencies[:MaximumDependencies],
	}
	result, err := candidate.BuildValid(context.Background(), publication)
	if err != nil {
		t.Fatalf("1024 dependencies: %v", err)
	}
	if got := len(mustResultProto(t, result).GetDependencies()); got != MaximumDependencies {
		t.Fatalf("dependency count = %d", got)
	}
	publication.Dependencies = dependencies
	if _, err := candidate.BuildValid(context.Background(), publication); !errors.Is(err, ErrInvariant) {
		t.Fatalf("1025 dependencies error = %v", err)
	}
}

func TestActiveIntrinsicResourceFieldsIncludeAppendedCharges(t *testing.T) {
	tests := []struct {
		name       string
		definition *opensplunk.KnowledgeObjectDefinition
		check      func(*testing.T, *opensplunk.KnowledgeResourceEstimate)
	}{
		{
			name:       "regex extraction outputs",
			definition: regexDefinition("regex-resources", "value"),
			check: func(t *testing.T, resources *opensplunk.KnowledgeResourceEstimate) {
				if resources.GetExtractionOutputs() != 1 || resources.GetRegexPrograms() != 1 ||
					resources.GetEstimatedRegexWorkUnits() == 0 || resources.GetJsonEvaluationWorkUnits() != 0 ||
					resources.GetScalarPredicates() != 0 {
					t.Fatalf("regex resources = %+v", resources)
				}
			},
		},
		{
			name:       "JSON evaluation work",
			definition: jsonDefinition("json-resources", "server.name"),
			check: func(t *testing.T, resources *opensplunk.KnowledgeResourceEstimate) {
				if resources.GetExtractionOutputs() != 1 || resources.GetJsonEvaluationWorkUnits() == 0 ||
					resources.GetRegexPrograms() != 0 || resources.GetScalarPredicates() != 0 {
					t.Fatalf("JSON resources = %+v", resources)
				}
			},
		},
		{
			name:       "scalar predicates",
			definition: calculatedDefinition("calculated-resources", `if(host="api", 1, 0)`),
			check: func(t *testing.T, resources *opensplunk.KnowledgeResourceEstimate) {
				if resources.GetScalarExpressions() != 1 || resources.GetScalarExpressionNodes() == 0 ||
					resources.GetScalarPredicates() == 0 || resources.GetExtractionOutputs() != 0 ||
					resources.GetJsonEvaluationWorkUnits() != 0 {
					t.Fatalf("calculated resources = %+v", resources)
				}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			candidate := mustActiveCandidate(t, test.definition)
			result, err := candidate.BuildValid(context.Background(), ActivePublication{
				Candidate: ExactIdentity{KnowledgeObjectID: "candidate-a", Version: 1},
			})
			if err != nil {
				t.Fatal(err)
			}
			test.check(t, mustResultProto(t, result).GetResources())
		})
	}
}
